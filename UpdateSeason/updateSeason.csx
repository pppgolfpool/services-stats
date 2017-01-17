#r "..\Common\PppPool.Common.dll"
#r "..\Common\Microsoft.WindowsAzure.Storage.dll"
#r "System.Xml.Linq"
#r "Newtonsoft.Json"

using System;
using System.Xml.Linq;
using System.Xml.XPath;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using PppPool.Common;

public static async Task Run(TimerInfo timer, TraceWriter log)
{
    var start = DateTime.UtcNow;
    var connectionString = "StatsStorage".GetEnvVar();
    var blobService = new BlobService(connectionString);

    var tournamentsUrl = "TournamentsUrl".GetEnvVar();
    var seasonData = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/Season", null, "ServiceToken".GetEnvVar());
    var season = (int)seasonData["Season"];
    var week = (int)seasonData["CurrentWeek"];

    var tournamentEntities = await GetTournaments();
    var profiles = await GetProfiles();

    List<JObject> tournaments = new List<JObject>();
    foreach (var t in tournamentEntities)
    {
        var tournamentFile = await blobService.DownloadBlobAsync("tournament", $"{season}/PGA TOUR/{t["Index"]}.json");
        tournaments.Add(JObject.Parse(tournamentFile));
    }

    var standings = new JArray();

    foreach (var profile in profiles)
    {
        var row = new JObject();
        row["UserId"] = (string)profile["UserId"];
        row["Name"] = (string)profile["Name"];
        row["Email"] = (string)profile["Email"];

        foreach (var tournament in tournaments)
        {
            var entity = tournamentEntities.Single(x => (string)x["PermanentNumber"] == (string)tournament["Id"]);
            if (!(bool)entity["Used"])
                continue;
            var poolie = ((JArray)tournament["Poolies"]).SingleOrDefault(x => (string)x["UserId"] == (string)row["UserId"]);
            row["Rank"] = (int)poolie["ProjectedRank"];
            row["Change"] = (int)poolie["Rank"] - (int)poolie["ProjectedRank"];
            row["Points"] = (double)poolie["YtdPoints"] + (double)poolie["Points"];
            if(!string.IsNullOrEmpty((string)poolie["Golfer"]))
            {
                var golfer = ((JArray)tournament["Golfers"]).SingleOrDefault(x => (string)x["Id"] == (string)poolie["Golfer"]);
                row["Wins"] = Convert.ToInt32(row["Wins"] ?? 0) + (((int)poolie["ProjectedRank"]) == 1 ? 1 : 0);
                row["Top5"] = Convert.ToInt32(row["Top5"] ?? 0) + (((int)poolie["ProjectedRank"]) <= 5 ? 1 : 0);
                row["Top10"] = Convert.ToInt32(row["Top10"] ?? 0) + (((int)poolie["ProjectedRank"]) <= 10 ? 1 : 0);
                row["Cuts"] = Convert.ToInt32(row["Cuts"] ?? 0) + (((string)golfer["Status"]).ToLower() == "cut" ? 1 : 0);
                row["PlusMinus"] = (int)row["Top5"] - (int)row["Cuts"];
            }
            else
            {
                row["Cuts"] = Convert.ToInt32(row["Cuts"] ?? 0) + 1;
            }
        }

        standings.Add(row);
    }

    var maxPoints = standings.Max(x => (double)x["Points"]);
    foreach (JObject row in standings)
    {
        row["Behind"] = maxPoints - (double)row["Points"];
    }

    var data = JArray.FromObject(((JArray)standings).OrderByDescending(x => (double)x["Points"]).ToList());

    await blobService.UploadBlobAsync("season", $"{season}/PGA TOUR/season.json", data.ToString(Formatting.Indented));

    var end = DateTime.UtcNow;
    log.Info($"Execution Time: {end - start}");
}

public static async Task<JArray> GetTournaments()
{
    var tournamentsUrl = "TournamentsUrl".GetEnvVar();
    var tournaments = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/GetTournaments", new Dictionary<string, string>
    {
        ["season"] = "current",
        ["tour"] = "PGA TOUR",
        ["key"] = "state",
        ["value"] = "dequeued,completed",
    }, "ServiceToken".GetEnvVar());
    return (JArray)tournaments;
}

public static async Task<List<JObject>> GetProfiles()
{
    var picksUrl = "UserUrl".GetEnvVar();
    List<JObject> list = new List<JObject>();
    JArray profiles = (JArray)(await RestService.AuthorizedPostAsync($"{picksUrl}/api/GetProfile", new Dictionary<string, string>
    {
        ["key"] = "all",
    }, "ServiceToken".GetEnvVar()));

    foreach (var item in profiles)
    {
        list.Add((JObject)item);
    }
    return list;
}