module Main

open System
open System.Collections.Generic
open System.CommandLine
open System.IO
open System.Net.Http
open System.Text
open System.Text.Json
open System.Text.Json.Serialization
open System.Threading.Tasks
open YamlDotNet.Serialization
open YamlDotNet.Serialization.NamingConventions


module CodeforcesApi =
    open System.Security.Cryptography


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

    type Member = {
        [<JsonPropertyName "handle">] Handle : string
        [<JsonPropertyName "name">] Name : string
    }

    [<JsonConverter(typeof<JsonStringEnumConverter>)>]
    type ParticipantType =
        | CONTESTANT = 1
        | PRACTICE = 2
        | VIRTUAL = 3
        | MANAGER = 4
        | OUT_OF_COMPETITION = 5

    type Party = {
        [<JsonPropertyName "members">] Members : Member[]
        [<JsonPropertyName "participantType">] ParticipantType : ParticipantType
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
    use httpClient = new HttpClient(BaseAddress = CodeforcesApi.baseUri)
    for contestId in contestIdList do
        let jsonFilePath = Path.Combine(dataDirPath, $"{contestId}.json")
        if File.Exists jsonFilePath && now - File.GetLastWriteTimeUtc jsonFilePath < timeThresh then
            Console.Error.WriteLine $"Existing file for {contestId} is less than {timeThresh} old, skipping"
        else
            Console.Error.WriteLine $"Attempting to download {contestId}..."
            let! json = CodeforcesApi.downloadStandings httpClient creds contestId
            Console.Error.WriteLine $"Done {contestId}"
            do! File.WriteAllTextAsync(jsonFilePath, json)
            do! Task.Delay 2000
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
    for i, (contest: CodeforcesApi.StandingsResult) in Seq.indexed contests do
        currentCol <- currentCol + 2 + contest.Problems.Length
        totalScoreExpr.Append(if i > 0 then ", " else "=SUM(") |> ignore
        totalScoreExpr.Append(formatCell false row currentCol) |> ignore
    totalScoreExpr.Append(if totalScoreExpr.Length > 0 then ")" else "0") |> ignore
    totalScoreExpr.ToString()


let formatScoreExpr col row len =
    if len < 1 then
        "=0"
    else
        let range = $"{formatCell false row col}:{formatCell false row (col + len - 1)}"
        $"=COUNTIF({range}, \"+\")+COUNTIF({range}, \"±\")"


let formatHeader baseCol baseRow (contests : CodeforcesApi.StandingsResult seq) =
    let l1 = StringBuilder "codeforces id\tratio\t∑"
    let l2 = StringBuilder $"\t=1\t{formatTotalScoreExpr (baseCol + 2) (baseRow + 1) contests}"
    for contest in contests do
        l1.Append "\t" |> ignore
        l2.Append "\t" |> ignore
        for i, p in Seq.indexed contest.Problems do
            l1.Append $"\t{(if i = 0 then contest.Contest.Name else String.Empty)}" |> ignore
            l2.Append $"\t{p.Index}" |> ignore

        let expected = contest.Problems |> Array.sumBy (fun p -> if p.Index.EndsWith ".1" || p.Index.EndsWith ".2" then 1 else 0)
        l1.Append "\t∑" |> ignore
        l2.Append $"\t{expected}" |> ignore
    $"{l1}\n{l2}"


let formatLine baseCol baseRow (lineIndex : int) (contestantId : string) (contests : CodeforcesApi.StandingsResult seq) =
    let tableRow = baseRow + 2 + lineIndex

    let sb = StringBuilder()
    let mutable currentCol = baseCol + 2
    for contest in contests do
        let markers = contest.Problems |> Array.map (fun _ -> 0)

        for ranklistRow in contest.Rows do
            if ranklistRow.Party.Members.Length = 1 && ranklistRow.Party.Members.[0].Handle = contestantId then
                let partType = ranklistRow.Party.ParticipantType
                for index, result in Seq.indexed ranklistRow.ProblemResults do
                    let problem = contest.Problems.[index]
                    if result.Points > 0.0 then
                        if partType = CodeforcesApi.ParticipantType.CONTESTANT then
                            markers.[index] <- 1
                        elif partType = CodeforcesApi.ParticipantType.PRACTICE && not (problem.Index.EndsWith ".1") && markers.[index] = 0 then
                            markers.[index] <- 2

        sb.Append "\t" |> ignore
        for value in markers do
            let marker =
                match value with
                | 1 -> "'+"
                | 2 -> "'±"
                | _ -> ""
            sb.Append $"\t{marker}" |> ignore

        sb.Append $"\t{formatScoreExpr (currentCol + 2) tableRow contest.Problems.Length}" |> ignore
        currentCol <- currentCol + 2 + contest.Problems.Length

    let ratioExpr = $"={formatCell false tableRow (baseCol + 2)}/{formatCell true (baseRow + 1) (baseCol + 2)}"
    let totalScoreExpr = formatTotalScoreExpr (baseCol + 2) tableRow contests
    $"{contestantId}\t{ratioExpr}\t{totalScoreExpr}{sb}"


type ScriptConfig () =
    [<YamlMember(Alias = "contestants")>]
    member val ContestantIdList : string[] = null with get, set

    [<YamlMember(Alias = "contests")>]
    member val ContestIdList : string[] = null with get, set

    [<YamlMember(Alias = "apikey")>]
    member val ApiKey = "" with get, set

    [<YamlMember(Alias = "apisecret")>]
    member val ApiSecret = "" with get, set


let readConfig (configPath : string) = task {
    let! text = File.ReadAllTextAsync configPath
    let deserializer =
        DeserializerBuilder()
            .IgnoreFields()
            .WithNamingConvention(CamelCaseNamingConvention.Instance)
            .Build()
    let cfg = deserializer.Deserialize<ScriptConfig> text
    return cfg
}


let handleDownload (configPath : string) (dataDirPath : string) : Task = task {
    let! cfg = readConfig configPath

    Directory.CreateDirectory(dataDirPath) |> ignore
    do! downloadData dataDirPath { ApiKey = cfg.ApiKey ; ApiSecret = cfg.ApiSecret } cfg.ContestIdList
}


let handleFormat (configPath : string) (dataDirPath : string) : Task = task {
    let! cfg = readConfig configPath
    let! data = readData dataDirPath cfg.ContestIdList

    let baseRow = 0
    let baseCol = 2
    use writer = File.CreateText "result.txt"
    do! writer.WriteLineAsync(formatHeader baseCol baseRow data)
    for index, contestantId in Seq.indexed cfg.ContestantIdList do
        do! writer.WriteLineAsync(formatLine baseCol baseRow index contestantId data)
}


let asyncMain (args : string[]) = task {
    let configPathOption = Option<string>("--config", "Path to YAML config", IsRequired = false)
    configPathOption.SetDefaultValue "config.yaml"

    let dataDirPathOption = new Option<string>("--data-dir", "JSONs directory", IsRequired = false)
    dataDirPathOption.SetDefaultValue "data"

    let downloadCommand = Command("download", "Download standings JSONs")
    downloadCommand.SetHandler(handleDownload, configPathOption, dataDirPathOption)

    let formatCommand = Command("format", "Format result.txt from downloaded JSONs")
    formatCommand.SetHandler(handleFormat, configPathOption, dataDirPathOption)

    let rootCommand = RootCommand()
    rootCommand.AddGlobalOption configPathOption
    rootCommand.AddGlobalOption dataDirPathOption
    rootCommand.Add downloadCommand
    rootCommand.Add formatCommand

    return! rootCommand.InvokeAsync(args)
}


[<EntryPoint>]
let main args =
    asyncMain(args).GetAwaiter().GetResult()
