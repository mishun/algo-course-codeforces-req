module Main

open System
open System.Collections.Generic
open System.CommandLine
open System.Diagnostics
open System.IO
open System.Net.Http
open System.Text
open System.Text.Json
open System.Text.RegularExpressions
open System.Threading.Tasks


module CodeforcesApi =
    open System.Security.Cryptography
    open System.Text.Json.Serialization


    let baseUri = Uri "https://codeforces.com/api/"


    type ApiCredentials = {
        ApiKey : string
        ApiSecret : string
    }

    let private formatStandingsRequest (creds : ApiCredentials) (contestId : string) =
        let unixTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        let requestRoot = $"contest.standings?apiKey={creds.ApiKey}&asManager=true&contestId={contestId}&showUnofficial=true&time={unixTime}"
        let randomToken = String(Random.Shared.GetItems([| 'a' .. 'z' |], 6))
        let hash =
            let bytes = Encoding.UTF8.GetBytes $"{randomToken}/{requestRoot}#{creds.ApiSecret}"
            use hash = SHA512.Create()
            Convert.ToHexString(hash.ComputeHash bytes).ToLower()
        Uri($"{requestRoot}&apiSig={randomToken}{hash}", UriKind.Relative)

    let downloadStandings (httpClient : HttpClient) creds contestId = task {
        let requestUri = formatStandingsRequest creds contestId
        use! httpResponse = httpClient.GetAsync(requestUri, HttpCompletionOption.ResponseHeadersRead)
        return! httpResponse.EnsureSuccessStatusCode().Content.ReadAsStringAsync()
    }

    type PartyMember = {
        [<JsonPropertyName "handle">] Handle : string
    }

    [<JsonConverter(typeof<JsonStringEnumConverter>)>]
    type ParticipantType =
        | CONTESTANT = 1
        | PRACTICE = 2
        | VIRTUAL = 3
        | MANAGER = 4
        | OUT_OF_COMPETITION = 5

    type Party = {
        [<JsonPropertyName "members">] Members : PartyMember[]
        [<JsonPropertyName "participantType">] ParticipantType : ParticipantType
        [<JsonPropertyName "ghost">] IsGhost : bool
    }

    type ProblemResult = {
        [<JsonPropertyName "points">] Points : float
        [<JsonPropertyName "rejectedAttemptCount">]  RejectedAttemptCount  : int
    }

    type RanklistRow = {
        [<JsonPropertyName "party">] Party : Party
        [<JsonPropertyName "rank">] Rank : int
        [<JsonPropertyName "points ">] Points : float
        [<JsonPropertyName "problemResults">] ProblemResults : ProblemResult[]
    }

    type Problem = {
        [<JsonPropertyName "index">] Index : string
        [<JsonPropertyName "name">] Name : string
        [<JsonPropertyName "type">] Type : string
    }

    type Contest = {
        [<JsonPropertyName "id">] Id : int
        [<JsonPropertyName "name">] Name : string
    }

    type StandingsResult = {
        [<JsonPropertyName "contest">] Contest : Contest
        [<JsonPropertyName "problems">] Problems : Problem[]
        [<JsonPropertyName "rows">] Rows : RanklistRow[]
    }

    type Result = {
        [<JsonPropertyName "status">] Status : string
        [<JsonPropertyName "result">] Result : StandingsResult
    }


let downloadData dataDirPath creds (contestIdList : string seq) = task {
    let now = DateTime.UtcNow
    let timeThresh = TimeSpan.FromMinutes 10.0

    let queue = Queue()
    for contestId in contestIdList do
        let jsonFilePath = Path.Combine(dataDirPath, $"{contestId}.json")
        if File.Exists jsonFilePath && now - File.GetLastWriteTimeUtc jsonFilePath < timeThresh then
            Console.Error.WriteLine $"Existing file for '\x1B[33;1m{contestId}\x1B[0m' is less than {timeThresh} old, skipping"
        else
            struct (contestId, jsonFilePath) |> queue.Enqueue

    if queue.Count > 0 then
        use httpClient = new HttpClient(BaseAddress = CodeforcesApi.baseUri)
        while queue.Count > 0 do
            let struct (contestId, jsonFilePath) = queue.Dequeue()
            Console.Error.WriteLine $"Attempting to download '\x1B[33;1m{contestId}\x1B[0m'..."

            let stopwatch = Stopwatch.StartNew()
            let! json = CodeforcesApi.downloadStandings httpClient creds contestId
            stopwatch.Stop()
            Console.Error.WriteLine $"\x1B[32;1mDone '{contestId}' in {stopwatch.ElapsedMilliseconds}ms\x1B[0m"

            do! File.WriteAllTextAsync(jsonFilePath, json)
            if queue.Count > 0 then
                let delay = 2000
                Console.Error.WriteLine $"Waiting for {delay}ms to meet codeforces throttling requirement..."
                do! Task.Delay delay
}


let readData dataDirPath (contestIdList : string seq) = task {
    let data = List<_>()
    for contestId in contestIdList do
        let jsonFilePath = Path.Combine(dataDirPath, $"{contestId}.json")
        let! bytes = File.ReadAllBytesAsync jsonFilePath
        let result = JsonSerializer.Deserialize<CodeforcesApi.Result>(bytes)
        data.Add result.Result
    return data
}


let printError (msg : string) =
    Console.Error.WriteLine $"\x1B[31;1m{msg}\x1B[0m"


let printWarn (msg : string) =
    Console.Error.WriteLine $"\x1B[33;1m{msg}\x1B[0m"


let printInfo (msg : string) =
    Console.Error.WriteLine msg


let validateData (problemIndexRegex : Regex) (listOfAllContestants : string[]) (contests : CodeforcesApi.StandingsResult seq) =
    let contestantLookup = HashSet listOfAllContestants

    let mutable totalNumberOfProblems = 0
    for contest in contests do
        let isUntested = Array.create contest.Problems.Length true
        for rowIndex, row in Seq.indexed contest.Rows do
            let msgPrefix =
                lazy
                    let sb = StringBuilder $"At '{contest.Contest.Id}'/row#{rowIndex:D3} {{"
                    if row.Party.Members <> null then
                        let mutable first = true
                        for m in row.Party.Members do
                            sb.Append(if first then "" else ", ").Append(m.Handle) |> ignore
                            first <- false
                    sb.Append "}:" |> ignore
                    sb.ToString()

            if row.Party.IsGhost then
                printError $"{msgPrefix.Value} is a ghost"

            if row.ProblemResults.Length <> contest.Problems.Length then
                printError $"{msgPrefix.Value} number of problem results ({row.ProblemResults.Length}) doesn't match number of problems in contest ({contest.Problems.Length})"

            match row.Party.ParticipantType with
            | CodeforcesApi.ParticipantType.VIRTUAL -> printWarn $"{msgPrefix.Value} virtual participant; contest config is wrong?"
            | CodeforcesApi.ParticipantType.OUT_OF_COMPETITION -> printWarn $"{msgPrefix.Value} out-of-competition participant"
            | CodeforcesApi.ParticipantType.MANAGER ->
                for problemIndex, res in Seq.indexed row.ProblemResults do
                    if res.Points > 0.0 then
                        isUntested.[problemIndex] <- false
            | _ when row.Party.Members.Length <> 1 ->
                printWarn $"{msgPrefix.Value} isn't single participant"
            | _ when not (contestantLookup.Contains row.Party.Members.[0].Handle) ->
                printWarn $"{msgPrefix.Value} isn't in list of contestants"
            | _ -> do ()

        for problemIndex, problem in Seq.indexed contest.Problems do
            let msgPrefix = lazy $"At '{contest.Contest.Id}/{problem.Index}':"

            totalNumberOfProblems <- totalNumberOfProblems + 1
            if problemIndexRegex.IsMatch problem.Index |> not then
                printError $"{msgPrefix.Value} problem index '{problem.Index}' doesn't match '{problemIndexRegex}'"
            if isUntested.[problemIndex] then
                printWarn $"{msgPrefix.Value} problem isn't tested by contest manager"

    printInfo $"{totalNumberOfProblems} problems total"


type Context = {
    BaseRow : int
    BaseCol : int
    CopypasteScore : float
    UpsolvabeRegex : Regex
    CountedInLimitRegex : Regex
}


let formatCell (absRow : bool) row col =
    let digits = List<_>()
    digits.Add(col % 26)
    let mutable tmp = col / 26
    while tmp > 0 do
        tmp <- tmp - 1
        digits.Add(tmp % 26)
        tmp <- tmp / 26
    digits.Reverse()

    let sb = StringBuilder()
    for d in digits do
        sb.Append(char (int 'A' + d)) |> ignore
    if absRow then
        sb.Append '$' |> ignore
    sb.Append(row + 1) |> ignore
    sb.ToString()


let formatTotalScoreExpr (baseCol : int) (row : int) (contests : CodeforcesApi.StandingsResult seq) =
    let totalScoreExpr = StringBuilder()
    let mutable currentCol = baseCol
    for i, contest in Seq.indexed contests do
        currentCol <- currentCol + 2 + contest.Problems.Length
        totalScoreExpr
            .Append(if i > 0 then ", " else "=SUM(")
            .Append(formatCell false row currentCol) |> ignore
    totalScoreExpr.Append(if totalScoreExpr.Length > 0 then ")" else "0").ToString()


let formatScoreExpr (ctx : Context) col row len =
    if len < 1 then
        "=0"
    else
        let range = $"{formatCell false row col}:{formatCell false row (col + len - 1)}"
        $"=COUNTIF({range}, \"+\")+COUNTIF({range}, \"±\")+({ctx.CopypasteScore})*COUNTIF({range}, \"!\")"


let formatHeader (ctx : Context) (contests : CodeforcesApi.StandingsResult seq) =
    let l1 = StringBuilder "codeforces id\tratio\t∑"
    let l2 = StringBuilder $"\t=1\t{formatTotalScoreExpr (ctx.BaseCol + 2) (ctx.BaseRow + 1) contests}"
    for contest in contests do
        l1.Append "\t" |> ignore
        l2.Append "\t" |> ignore
        for i, p in Seq.indexed contest.Problems do
            l1.Append $"\t{(if i = 0 then contest.Contest.Name else String.Empty)}" |> ignore
            l2.Append $"\t{p.Index}" |> ignore

        let expected = contest.Problems |> Array.sumBy (fun p -> if ctx.CountedInLimitRegex.IsMatch p.Index then 1 else 0)
        l1.Append "\t∑" |> ignore
        l2.Append $"\t{expected}" |> ignore
    $"{l1}\n{l2}"


[<Struct; RequireQualifiedAccess>]
type Marker =
    | Tainted | Clear | Attempted | Ok | OkLate
with
    member marker.Rendered =
        match marker with
        | Ok -> "'+"
        | Tainted -> "'!"
        | OkLate -> "'±"
        | Attempted -> "'-"
        | Clear -> ""

    member marker.Priority =
        match marker with
        | Tainted -> 1000
        | Ok -> 100
        | OkLate -> 50
        | Attempted -> 10
        | Clear -> 0

    static member UpdateWith (marker : byref<Marker>, res : CodeforcesApi.ProblemResult, late : bool) =
        let proposed =
            match late with
            | false when res.Points > 0.0 -> Ok
            | true when res.Points > 0.0 -> OkLate
            | _ when res.RejectedAttemptCount > 0 -> Attempted
            | _ -> Clear

        if proposed.Priority > marker.Priority then
            marker <- proposed

    static member Initial (tainted : bool) =
        if tainted then Tainted else Clear


let formatLine (ctx : Context) (lineIndex : int) (contestantId : string) (contests : CodeforcesApi.StandingsResult seq) (taintedProblems : HashSet<string>) =
    let tableRow = ctx.BaseRow + 2 + lineIndex

    let sb = StringBuilder()
    let mutable currentCol = ctx.BaseCol + 2
    let mutable problemsAvailable = 0
    let mutable problemsScored = 0
    for contest in contests do
        let isUpsolvable = [| for p in contest.Problems -> ctx.UpsolvabeRegex.IsMatch p.Index |]

        let markers = [|
            for p in contest.Problems ->
                Marker.Initial(taintedProblems.Contains $"{contest.Contest.Id}/{p.Index}")
        |]

        for ranklistRow in contest.Rows do
            if ranklistRow.Party.Members.Length = 1 && ranklistRow.Party.Members.[0].Handle = contestantId then
                match ranklistRow.Party.ParticipantType with
                | CodeforcesApi.ParticipantType.CONTESTANT ->
                    for index, result in Seq.indexed ranklistRow.ProblemResults do
                        Marker.UpdateWith(&markers.[index], result, false)

                | CodeforcesApi.ParticipantType.PRACTICE ->
                    for problemIndex, result in Seq.indexed ranklistRow.ProblemResults do
                        if isUpsolvable.[problemIndex] then
                            Marker.UpdateWith(&markers.[problemIndex], result, true)

                | _ -> ()

        sb.Append "\t" |> ignore
        for marker in markers do
            sb.Append("\t").Append(marker.Rendered) |> ignore
            problemsAvailable <- problemsAvailable + (match marker with | Marker.Clear | Marker.Attempted -> 1 | _ -> 0)
            problemsScored <- problemsScored + (match marker with | Marker.Ok | Marker.OkLate -> 1 | Marker.Tainted -> -1 | _ -> 0)

        sb.Append $"\t{formatScoreExpr ctx (currentCol + 2) tableRow contest.Problems.Length}" |> ignore
        currentCol <- currentCol + 2 + contest.Problems.Length

    printInfo $"{contestantId} scored {problemsScored} has {problemsAvailable} problems available"

    let ratioExpr = $"={formatCell false tableRow (ctx.BaseCol + 2)}/{formatCell true (ctx.BaseRow + 1) (ctx.BaseCol + 2)}"
    let totalScoreExpr = formatTotalScoreExpr (ctx.BaseCol + 2) tableRow contests
    $"{contestantId}\t{ratioExpr}\t{totalScoreExpr}{sb}"


let prapareCopypastes (listOfAllContestants : string[]) (contests : CodeforcesApi.StandingsResult seq) (copypastes : IDictionary<string, string[]>) =
    let contestantLookup = HashSet listOfAllContestants
    let problemCodeLookup =
        HashSet (seq {
            for c in contests do
                for p in c.Problems do
                    yield $"{c.Contest.Id}/{p.Index}"
        })

    let problemsByContestant = readOnlyDict (seq { for id in listOfAllContestants -> (id, HashSet<string>()) })
    if copypastes <> null then
        for KeyValue(problemCode, listedContestants) in copypastes do
            if String.IsNullOrWhiteSpace problemCode then
                printError "Key is null or empty"
            elif problemCodeLookup.Contains problemCode then
                if listedContestants = null then
                    printError $"at '{problemCode}': contestant list is null"
                else
                    for contestantId in listedContestants do
                        if contestantLookup.Contains contestantId then
                            problemsByContestant.[contestantId].Add problemCode |> ignore
                        else
                            printError $"at '{problemCode}': '{contestantId}' is not a valid contestant"
            else
                printError $"'{problemCode}' is not valid problem code"

    problemsByContestant


module Config =
    open YamlDotNet.Core
    open YamlDotNet.Core.Events
    open YamlDotNet.Serialization

    [<CLIMutable>]
    type CodeforcesApiSecret = {
        [<YamlMember(Alias = "codeforces-api-key")>]
        CodeforcesApiKey : string

        [<YamlMember(Alias = "codeforces-api-secret")>]
        CodeforcesApiSecret : string
    }

    [<CLIMutable>]
    type ContestConfig = {
        [<YamlMember(Alias = "id")>]
        TableId : string

        [<YamlMember(Alias = "contestants")>]
        ContestantIdList : string[]

        [<YamlMember(Alias = "contests")>]
        ContestIdList : string[]

        [<YamlMember(Alias = "copypaste")>]
        Copypastes : IDictionary<string, string[]>

        [<YamlMember(Alias = "copypaste-score")>]
        CopypasteScore : Nullable<float>
    }


    let private deserializer = DeserializerBuilder().IgnoreFields().Build()

    let private readFromFile<'t> (path : string) = task {
        let! text = File.ReadAllTextAsync path
        return deserializer.Deserialize<'t> text
    }

    let private readManyFromFile<'t> (path : string) = task {
        let! text = File.ReadAllTextAsync path
        use sr = new StringReader(text)
        let parser = Parser(sr)
        let res = List<'t>()
        let streamStart = parser.Consume<StreamStart>()
        let mutable docStart = Unchecked.defaultof<DocumentStart>
        while parser.Accept<DocumentStart>(&docStart) do
            let item = deserializer.Deserialize<'t>(parser)
            res.Add item
        return res.ToArray()
    }


    let readConfig path = readFromFile<ContestConfig> path
    let readConfig2 path = readManyFromFile<ContestConfig> path

    let readSecret path = readFromFile<CodeforcesApiSecret> path


let handleDownload (secretPath : string) (configPath : string) (dataDirPath : string) : Task = task {
    let! cfg = Config.readConfig configPath
    let! secret = Config.readSecret secretPath

    Directory.CreateDirectory(dataDirPath) |> ignore
    do! downloadData dataDirPath { ApiKey = secret.CodeforcesApiKey ; ApiSecret = secret.CodeforcesApiSecret } cfg.ContestIdList
}


let handleFormat (configPath : string) (dataDirPath : string) (resultPathFromConfig : string) : Task = task {
    let! cfg2 = Config.readConfig2 configPath
    let cfg = cfg2.[0]
    //let! cfg = Config.readConfig configPath

    let! contests = readData dataDirPath cfg.ContestIdList
    let problemIndexRegex = Regex @"[A-Z]+\.[1-3]"
    validateData problemIndexRegex cfg.ContestantIdList contests
    let copypastes = prapareCopypastes cfg.ContestantIdList contests cfg.Copypastes

    let ctx = {
        BaseRow = 0
        BaseCol = 2
        CopypasteScore = cfg.CopypasteScore.GetValueOrDefault -1.0
        UpsolvabeRegex = Regex @"[A-Z]+\.[2-3]"
        CountedInLimitRegex = Regex @"[A-Z]+\.[1-2]"
    }

    let resultPath =
        match resultPathFromConfig with
        | s when String.IsNullOrWhiteSpace s -> $"result-{cfg.TableId}.txt"
        | s -> s

    use writer = File.CreateText resultPath
    do! writer.WriteLineAsync(formatHeader ctx contests)
    for index, contestantId in Seq.indexed cfg.ContestantIdList do
        let tableLine = formatLine ctx index contestantId contests copypastes.[contestantId]
        do! writer.WriteLineAsync tableLine
    printInfo $"Table is written to '{resultPath}'"
}


let handleDraftCopypaste (configPath : string) (dataDirPath : string) : Task = task {
    let! cfg = Config.readConfig configPath
    let resultPath = "copypaste-draft.md"

    let! contests = readData dataDirPath cfg.ContestIdList
    use writer = File.CreateText resultPath
    for contest in contests do
        do! writer.WriteLineAsync $"# Contest {contest.Contest.Id} ({contest.Contest.Name})"
        for problem in contest.Problems do
            do! writer.WriteLineAsync $"## Problem {contest.Contest.Id}/{problem.Index} ({problem.Name})"
            do! writer.WriteLineAsync()
        do! writer.WriteLineAsync()
}


let prepareRootCommand () =
    let secretPathOption = Option<string>("--secrets", "Path to Codeforces API credentials", IsRequired = false)
    secretPathOption.SetDefaultValue "secret.yaml"

    let configPathOption = Option<string>("--config", "Path to YAML config", IsRequired = false)
    configPathOption.SetDefaultValue "config.yaml"

    let dataDirPathOption = new Option<string>("--data-dir", "JSONs directory", IsRequired = false)
    dataDirPathOption.SetDefaultValue "data"

    let resultPathOption = Option<string>("--result", "Path to result file", IsRequired = false)

    let downloadCommand = Command("download", "Download standings JSONs")
    downloadCommand.AddOption secretPathOption
    downloadCommand.SetHandler(handleDownload, secretPathOption, configPathOption, dataDirPathOption)

    let formatCommand = Command("format", "Format result from downloaded JSONs")
    formatCommand.AddOption resultPathOption
    formatCommand.SetHandler(handleFormat, configPathOption, dataDirPathOption, resultPathOption)

    let draftCopypaste = Command("draft-copypaste", "Create draft for copypaste.md")
    draftCopypaste.SetHandler(handleDraftCopypaste, configPathOption, dataDirPathOption)

    let rootCommand = RootCommand()
    seq { configPathOption ; dataDirPathOption } |> Seq.iter rootCommand.AddGlobalOption
    seq { downloadCommand ; formatCommand ; draftCopypaste } |> Seq.iter rootCommand.Add
    rootCommand


[<EntryPoint>]
let main args =
    prepareRootCommand().InvokeAsync(args).GetAwaiter().GetResult()
