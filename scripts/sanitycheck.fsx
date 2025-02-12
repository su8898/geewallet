#!/usr/bin/env fsharpi

open System
open System.IO
open System.Linq
open System.Diagnostics

open System.Text
open System.Text.RegularExpressions
#r "System.Core.dll"
open System.Xml
#r "System.Xml.Linq.dll"
open System.Xml.Linq
open System.Xml.XPath

#r "System.Configuration"
open System.Configuration
#load "InfraLib/Misc.fs"
#load "InfraLib/Process.fs"
#load "InfraLib/Git.fs"
open FSX.Infrastructure
open Process

#load "fsxHelper.fs"
open GWallet.Scripting

#r "../.nuget/packages/Microsoft.Build.16.11.0/lib/net472/Microsoft.Build.dll"
open Microsoft.Build.Construction


module MapHelper =
    let GetKeysOfMap (map: Map<'K,'V>): seq<'K> =
        map |> Map.toSeq |> Seq.map fst

    let MergeIntoMap<'K,'V when 'K: comparison> (from: seq<'K*'V>): Map<'K,seq<'V>> =
        let keys = from.Select (fun (k, v) -> k)
        let keyValuePairs =
            seq {
                for key in keys do
                    let valsForKey = (from.Where (fun (k, v) -> key = k)).Select (fun (k, v) -> v) |> seq
                    yield key,valsForKey
            }
        keyValuePairs |> Map.ofSeq

[<StructuralEquality; StructuralComparison>]
type private PackageInfo =
    {
        PackageId: string
        PackageVersion: string
        ReqReinstall: Option<bool>
    }

type private DependencyHolder =
    { Name: string }

[<CustomComparison; CustomEquality>]
type private ComparableFileInfo =
    {
        File: FileInfo
    }
    member self.DependencyHolderName: DependencyHolder =
        if self.File.FullName.ToLower().EndsWith ".nuspec" then
            { Name = self.File.Name }
        else
            { Name = self.File.Directory.Name + "/" }

    interface IComparable with
        member this.CompareTo obj =
            match obj with
            | null -> this.File.FullName.CompareTo null
            | :? ComparableFileInfo as other -> this.File.FullName.CompareTo other.File.FullName
            | _ -> invalidArg "obj" "not a ComparableFileInfo"
    override this.Equals obj =
        match obj with
        | :? ComparableFileInfo as other ->
            this.File.FullName.Equals other.File.FullName
        | _ -> false
    override this.GetHashCode () =
        this.File.FullName.GetHashCode ()

let FindOffendingPrintfUsage () =
    let findScript = Path.Combine (FsxHelper.RootDir.FullName, "scripts", "find.fsx")
    let excludeFolders =
        String.Format (
            "scripts{0}" +
            "src{1}GWallet.Frontend.Console{0}" +
            "src{1}GWallet.Backend.Tests{0}" +
            "src{1}GWallet.Backend{1}FSharpUtil.fs",
            Path.PathSeparator,
            Path.DirectorySeparatorChar
        )

    let proc =
        {
            Command = FsxHelper.FsxRunner
            Arguments = sprintf "%s --exclude=%s %s"
                                findScript
                                excludeFolders
                                "printf failwithf"
        }
    let findProc = Process.SafeExecute (proc, Echo.All)
    if findProc.Output.StdOut.Trim().Length > 0 then
        Console.Error.WriteLine "Illegal usage of printf/printfn/sprintf/sprintfn/failwithf detected; use SPrintF1/SPrintF2/... instead"
        Environment.Exit 1

let SanityCheckNugetPackages () =

    let notSubmodule (dir: DirectoryInfo): bool =
        let getSubmoduleDirsForThisRepo (): seq<DirectoryInfo> =
            let regex = Regex("path\s*=\s*([^\s]+)")
            seq {
                for regexMatch in regex.Matches (File.ReadAllText (".gitmodules")) do
                    let submoduleFolderRelativePath = regexMatch.Groups.[1].ToString ()
                    let submoduleFolder =
                        DirectoryInfo (
                            Path.Combine (Directory.GetCurrentDirectory (), submoduleFolderRelativePath)
                        )
                    yield submoduleFolder
            }
        not (getSubmoduleDirsForThisRepo().Any (fun d -> dir.FullName = d.FullName))

    // this seems to be a bug in Microsoft.Build nuget library, FIXME: report
    let normalizeDirSeparatorsPaths (path: string): string =
        path
            .Replace('\\', Path.DirectorySeparatorChar)
            .Replace('/', Path.DirectorySeparatorChar)

    let sanityCheckNugetPackagesFromSolution (sol: FileInfo) =
        let findPackagesDotConfigFiles (): seq<FileInfo> =
            let parsedSolution = SolutionFile.Parse sol.FullName
            seq {
                for projPath in (parsedSolution.ProjectsInOrder.Select(fun proj -> normalizeDirSeparatorsPaths proj.AbsolutePath).ToList()) do
                    if projPath.ToLower().EndsWith ".fsproj" then
                        for file in ((FileInfo projPath).Directory).EnumerateFiles () do
                            if file.Name.ToLower () = "packages.config" then
                                yield file
            }

        let rec findNuspecFiles (dir: DirectoryInfo): seq<FileInfo> =
            dir.Refresh ()
            seq {
                for file in dir.EnumerateFiles () do
                    if (file.Name.ToLower ()).EndsWith ".nuspec" then
                        yield file
                for subdir in dir.EnumerateDirectories().Where notSubmodule do
                    for file in findNuspecFiles subdir do
                        yield file
            }

        let getPackageTree (sol: FileInfo): Map<ComparableFileInfo,seq<PackageInfo>> =
            let packagesConfigFiles = findPackagesDotConfigFiles()
            let packageConfigFileElements =
                seq {
                    for packagesConfigFile in packagesConfigFiles do
                        let xmlDoc = XDocument.Load packagesConfigFile.FullName
                        for descendant in xmlDoc.Descendants () do
                            if descendant.Name.LocalName.ToLower() = "package" then
                                let id = descendant.Attributes().Single(fun attr -> attr.Name.LocalName = "id").Value
                                let version = descendant.Attributes().Single(fun attr -> attr.Name.LocalName = "version").Value
                                let reqReinstall = descendant.Attributes().Any(fun attr -> attr.Name.LocalName = "requireReinstallation")
                                yield { File = packagesConfigFile }, { PackageId = id; PackageVersion = version; ReqReinstall = Some reqReinstall }
                } |> List.ofSeq

            let solDir = sol.Directory
            solDir.Refresh ()
            let nuspecFiles = findNuspecFiles solDir
            let nuspecFileElements =
                seq {
                    for nuspecFile in nuspecFiles do
                        let xmlDoc = XDocument.Load nuspecFile.FullName

                        let nsOpt =
                            let nsString = xmlDoc.Root.Name.Namespace.ToString()
                            if String.IsNullOrEmpty nsString then
                                None
                            else
                                let nsManager = XmlNamespaceManager(NameTable())
                                let nsPrefix = "x"
                                nsManager.AddNamespace(nsPrefix, nsString)
                                if nsString <> "http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd" then
                                    Console.Error.WriteLine "Warning: the namespace URL doesn't match expectations, nuspec's XPath query may result in no elements"
                                Some(nsManager, sprintf "%s:" nsPrefix)
                        let query = "//{0}dependency"
                        let dependencies =
                            match nsOpt with
                            | None ->
                                let fixedQuery = String.Format(query, String.Empty)
                                xmlDoc.XPathSelectElements fixedQuery
                            | Some (nsManager, nsPrefix) ->
                                let fixedQuery = String.Format(query, nsPrefix)
                                xmlDoc.XPathSelectElements(fixedQuery, nsManager)

                        for dependency in dependencies do
                            let id = dependency.Attributes().Single(fun attr -> attr.Name.LocalName = "id").Value
                            let version = dependency.Attributes().Single(fun attr -> attr.Name.LocalName = "version").Value
                            yield { File = nuspecFile }, { PackageId = id; PackageVersion = version; ReqReinstall = None }
                } |> List.ofSeq

            let allElements = Seq.append packageConfigFileElements nuspecFileElements

            allElements
            |> MapHelper.MergeIntoMap

        let getAllPackageIdsAndVersions (packageTree: Map<ComparableFileInfo,seq<PackageInfo>>): Map<PackageInfo,seq<DependencyHolder>> =
            seq {
                for KeyValue (dependencyHolderFile, pkgs) in packageTree do
                    for pkg in pkgs do
                        yield pkg, dependencyHolderFile.DependencyHolderName
            } |> MapHelper.MergeIntoMap

        let getDirectoryNamesForPackagesSet (packages: Map<PackageInfo,seq<DependencyHolder>>): Map<string,seq<DependencyHolder>> =
            seq {
                for KeyValue (package, prjs) in packages do
                    yield (sprintf "%s.%s" package.PackageId package.PackageVersion), prjs
            } |> Map.ofSeq

        let findMissingPackageDirs (solDir: DirectoryInfo) (idealPackageDirs: Map<string,seq<DependencyHolder>>): Map<string,seq<DependencyHolder>> =
            solDir.Refresh ()
            if not FsxHelper.NugetSolutionPackagesDir.Exists then
                failwithf "'%s' subdir under solution dir %s doesn't exist, run `make` first"
                    FsxHelper.NugetSolutionPackagesDir.Name
                    FsxHelper.NugetSolutionPackagesDir.FullName
            let packageDirsAbsolutePaths = FsxHelper.NugetSolutionPackagesDir.EnumerateDirectories().Select (fun dir -> dir.FullName)
            if not (packageDirsAbsolutePaths.Any()) then
                Console.Error.WriteLine (
                    sprintf "'%s' subdir under solution dir %s doesn't contain any packages"
                        FsxHelper.NugetSolutionPackagesDir.Name
                        FsxHelper.NugetSolutionPackagesDir.FullName
                )
                Console.Error.WriteLine "Forgot to `git submodule update --init`?"
                Environment.Exit 1

            seq {
                for KeyValue (packageDirNameThatShouldExist, prjs) in idealPackageDirs do
                    if not (packageDirsAbsolutePaths.Contains (Path.Combine(FsxHelper.NugetSolutionPackagesDir.FullName, packageDirNameThatShouldExist))) then
                        yield packageDirNameThatShouldExist, prjs
            } |> Map.ofSeq

        let findExcessPackageDirs (solDir: DirectoryInfo) (idealPackageDirs: Map<string,seq<DependencyHolder>>): seq<string> =
            solDir.Refresh ()
            if not (FsxHelper.NugetSolutionPackagesDir.Exists) then
                failwithf "'%s' subdir under solution dir %s doesn't exist, run `make` first"
                    FsxHelper.NugetSolutionPackagesDir.Name
                    FsxHelper.NugetSolutionPackagesDir.FullName
            // "src" is a directory for source codes and build scripts,
            // not for packages, so we need to exclude it from here
            let packageDirNames = FsxHelper.NugetSolutionPackagesDir.EnumerateDirectories().Select(fun dir -> dir.Name).Except(["src"])
            if not (packageDirNames.Any()) then
                failwithf "'%s' subdir under solution dir %s doesn't contain any packages"
                    FsxHelper.NugetSolutionPackagesDir.Name
                    FsxHelper.NugetSolutionPackagesDir.FullName
            let packageDirsThatShouldExist = MapHelper.GetKeysOfMap idealPackageDirs
            seq {
                for packageDirThatExists in packageDirNames do
                    if not (packageDirsThatShouldExist.Contains packageDirThatExists) then
                        yield packageDirThatExists
            }

        let findPackagesWithMoreThanOneVersion
            (packageTree: Map<ComparableFileInfo,seq<PackageInfo>>)
            : Map<string,seq<ComparableFileInfo*PackageInfo>> =

            let getAllPackageInfos (packages: Map<ComparableFileInfo,seq<PackageInfo>>) =
                let pkgInfos =
                    seq {
                        for KeyValue (_, pkgs) in packages do
                            for pkg in pkgs do
                                yield pkg
                    }
                Set pkgInfos

            let getAllPackageVersionsForPackageId (packages: seq<PackageInfo>) (packageId: string) =
                seq {
                    for package in packages do
                        if package.PackageId = packageId then
                            yield package.PackageVersion
                } |> Set

            let packageInfos = getAllPackageInfos packageTree
            let packageIdsWithMoreThan1Version =
                seq {
                    for packageId in packageInfos.Select (fun pkg -> pkg.PackageId) do
                        let versions = getAllPackageVersionsForPackageId packageInfos packageId
                        if versions.Count > 1 then
                            yield packageId
                }
            if not (packageIdsWithMoreThan1Version.Any()) then
                Map.empty
            else
                seq {
                    for pkgId in packageIdsWithMoreThan1Version do
                        let pkgs = seq {
                            for KeyValue (file, packageInfos) in packageTree do
                                for pkg in packageInfos do
                                    if pkg.PackageId = pkgId then
                                        yield file, pkg
                        }
                        yield pkgId, pkgs
                } |> Map.ofSeq

        let packageTree = getPackageTree sol
        let packages = getAllPackageIdsAndVersions packageTree
        Console.WriteLine(sprintf "%d nuget packages found for solution %s" packages.Count sol.Name)
        let idealDirList = getDirectoryNamesForPackagesSet packages
        
        let solDir = sol.Directory
        solDir.Refresh ()
        let missingPackageDirs = findMissingPackageDirs solDir idealDirList
        if missingPackageDirs.Any () then
            for KeyValue(missingPkg, depHolders) in missingPackageDirs do
                let depHolderNames = String.Join(",", depHolders.Select(fun dh -> dh.Name))
                Console.Error.WriteLine (sprintf "Missing folder for nuget package in submodule: %s (referenced from %s)" missingPkg depHolderNames)
            Environment.Exit 1

        let excessPackageDirs = findExcessPackageDirs solDir idealDirList
        if excessPackageDirs.Any () then
            let advice = "remove it with git filter-branch to avoid needless bandwidth: http://stackoverflow.com/a/17824718/6503091"
            for excessPkg in excessPackageDirs do
                Console.Error.WriteLine(sprintf "Unused nuget package folder: %s (%s)" excessPkg advice)
            Environment.Exit 1

        let pkgWithMoreThan1VersionPrint (key: string) (packageInfos: seq<ComparableFileInfo*PackageInfo>) =
            Console.Error.WriteLine (sprintf "Package found with more than one version: %s. All occurrences:" key)
            for file,pkgInfo in packageInfos do
                Console.Error.WriteLine (sprintf "* Version: %s. Dependency holder: %s" pkgInfo.PackageVersion file.DependencyHolderName.Name)
        let packagesWithMoreThanOneVersion = findPackagesWithMoreThanOneVersion packageTree
        if packagesWithMoreThanOneVersion.Any() then
            Map.iter pkgWithMoreThan1VersionPrint packagesWithMoreThanOneVersion
            Environment.Exit 1

        let findPackagesWithSomeReqReinstallAttrib
            (packageTree: Map<ComparableFileInfo,seq<PackageInfo>>)
            : seq<ComparableFileInfo*PackageInfo> =
            seq {
                for KeyValue (file, packageInfos) in packageTree do
                    for pkg in packageInfos do
                        match pkg.ReqReinstall with
                        | Some true ->
                            yield file, pkg
                        | _ -> ()
            }
        let packagesWithWithSomeReqReinstallAttrib = findPackagesWithSomeReqReinstallAttrib packageTree
        if packagesWithWithSomeReqReinstallAttrib.Any() then
            Console.Error.WriteLine (
                sprintf "Packages found with some RequireReinstall attribute (please reinstall it before pushing):"
            )
            for file,pkg in packagesWithWithSomeReqReinstallAttrib do
                Console.Error.WriteLine (
                    sprintf "* Name: %s. Project: %s" pkg.PackageId file.DependencyHolderName.Name
                )
            Environment.Exit 1

        Console.WriteLine (sprintf "Nuget sanity check succeeded for solution dir %s" solDir.FullName)


    let rec findSolutions (dir: DirectoryInfo): seq<FileInfo> =
        dir.Refresh ()
        seq {
            // FIXME: avoid returning duplicates? (in case there are 2 .sln files in the same dir...)
            for file in dir.EnumerateFiles () do
                if file.Name.ToLower().EndsWith ".sln" then
                    yield file
            for subdir in dir.EnumerateDirectories().Where notSubmodule do
                for solutionDir in findSolutions subdir do
                    yield solutionDir
        }

    let solutions = Directory.GetCurrentDirectory() |> DirectoryInfo |> findSolutions
    for sol in solutions do
        sanityCheckNugetPackagesFromSolution sol

FindOffendingPrintfUsage()
SanityCheckNugetPackages()

