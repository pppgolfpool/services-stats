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
using System.Dynamic;

public static async Task Run(TimerInfo timer, TraceWriter log)
{
    var start = DateTime.UtcNow;
    var connectionString = "StatsStorage".GetEnvVar();
    var blobService = new BlobService(connectionString);

    var tour = "PGA TOUR";
    var tournamentsUrl = "TournamentsUrl".GetEnvVar();
    var seasonData = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/Season", null, "ServiceToken".GetEnvVar());
    var season = (int)seasonData["Season"];
    var week = (int)seasonData["CurrentWeek"];

    // We only need previous or current tournaments. Not picking either because nothing has happened.
    var tournaments = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/GetTournaments", new Dictionary<string, string>
    {
        ["season"] = "current",
        ["tour"] = tour,
        ["key"] = "state",
        ["value"] = "dequeued,completed",
    }, "ServiceToken".GetEnvVar());

    dynamic profiles = await GetProfiles();
    JArray pooliesStatsFile = new JArray();
    JArray monthsStatsFile = new JArray();

    foreach (var profile in profiles)
    {
        var nameSplit = ((string)profile["Name"]).Split(new[] { ' ' });
        var lastFirst = nameSplit.Last() + ", " + nameSplit.First();
        dynamic profileStats = JObject.FromObject(new
        {
            UserId = profile.UserId,
            Name = profile.Name,
            Email = profile.Email,
            LastFirst = lastFirst,
            Points = new JArray(),
        });
        pooliesStatsFile.Add(profileStats);
    }

    // get Months that will be calculated (current and previous)
    for (int month = 1; month <= DateTime.UtcNow.Month; month++)
    {
        var monthTournaments = tournaments.Where(x => ((DateTime)x["End"]).Month == month);

        List<JObject> statsFiles = new List<JObject>();
        foreach (var tournament in monthTournaments)
        {
            var stats = await blobService.DownloadBlobAsync("tournament", $"{season}/{tour}/{tournament["Index"]}.json");
            statsFiles.Add(JObject.Parse(stats));
        }

        var monthArray = new JArray();

        foreach (var poolieStats in pooliesStatsFile)
        {
            double monthPoints = 0.0d;
            foreach (var statFile in statsFiles)
            {
                var poolie = ((JArray)statFile["Poolies"]).FirstOrDefault(x => (string)x["UserId"] == (string)poolieStats["UserId"]);
                if(poolie != null)
                    monthPoints += (double)poolie["Points"];
            }
            ((JArray)poolieStats["Points"]).Add(monthPoints);

            var nameSplit = ((string)poolieStats["Name"]).Split(new[] { ' ' });
            var lastFirst = nameSplit.Last() + ", " + nameSplit.First();
            monthArray.Add(JObject.FromObject(new
            {
                UserId = (string)poolieStats["UserId"],
                Name = (string)poolieStats["Name"],
                Email = (string)poolieStats["Email"],
                LastFirst = lastFirst,
                Points = monthPoints,
            }));
        }

        monthsStatsFile.Add(JArray.FromObject(monthArray.OrderByDescending(x => (double)x["Points"])));
    }

    var data = JArray.FromObject(pooliesStatsFile.OrderByDescending(x => ((JArray)x["Points"])[0]).ToList());

    await blobService.UploadBlobAsync("monthly", $"{season}/{tour}/poolie.json", data.ToString(Formatting.Indented));
    await blobService.UploadBlobAsync("monthly", $"{season}/{tour}/month.json", monthsStatsFile.ToString(Formatting.Indented));

    var end = DateTime.UtcNow;
    log.Info($"Execution time: {end - start}");
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