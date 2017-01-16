﻿#r "..\Common\PppPool.Common.dll"
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

// season, tour, key(index, id, current, week), value=query parameter
public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    var jwt = await req.GetJwt("submitter");
    if (jwt == null)
        return req.CreateError(HttpStatusCode.Unauthorized);

    IDictionary<string, string> query = req.GetQueryNameValuePairs().ToDictionary(pair => pair.Key, pair => pair.Value);

    var connectionString = "StatsStorage".GetEnvVar();
    var blobService = new BlobService(connectionString);

    var tournamentsUrl = "TournamentsUrl".GetEnvVar();
    var seasonData = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/Season", null, "ServiceToken".GetEnvVar());
    var season = (int)seasonData["Season"];
    var week = (int)seasonData["CurrentWeek"];

    if(query["season"].ToLower() != "current")
    {
        season = Convert.ToInt32(query["season"]);
    }

    var tour = query["tour"];
    var key = query["key"].ToLower();
    var value = string.Empty;

    if (key != "current")
        value = query["value"].ToLower();

    JObject data = null;

    if(key == "index")
    {
        data = JObject.Parse(await blobService.DownloadBlobAsync("tournament", $"{season}/{tour}/{value}.json"));
    }

    if(key == "id")
    {
        var tournament = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/GetTournaments", new Dictionary<string, string>
        {
            ["season"] = "current",
            ["tour"] = "PGA TOUR",
            ["key"] = "id",
            ["value"] = value,
        }, "ServiceToken".GetEnvVar());
        if (tournament == null)
            return req.CreateError(HttpStatusCode.BadRequest);
        data = JObject.Parse(await blobService.DownloadBlobAsync("tournament", $"{season}/{tour}/{(string)tournament["Index"]}.json"));
    }

    if(key == "current")
    {
        var tournaments = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/GetTournaments", new Dictionary<string, string>
        {
            ["season"] = "current",
            ["tour"] = "PGA TOUR",
            ["key"] = "state",
            ["value"] = "progressing,completed",
        }, "ServiceToken".GetEnvVar());
        var tournament = ((JArray)tournaments).FirstOrDefault(); // should only be one.
        if (tournament == null)
            return req.CreateError(HttpStatusCode.BadRequest);
        data = JObject.Parse(await blobService.DownloadBlobAsync("tournament", $"{season}/{tour}/{(string)tournament["Index"]}.json"));
    }

    if(key == "week")
    {
        var tournament = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/GetTournaments", new Dictionary<string, string>
        {
            ["season"] = "current",
            ["tour"] = "PGA TOUR",
            ["key"] = "week",
            ["value"] = value,
        }, "ServiceToken".GetEnvVar());
        if (tournament == null)
            return req.CreateError(HttpStatusCode.BadRequest);
        data = JObject.Parse(await blobService.DownloadBlobAsync("tournament", $"{season}/{tour}/{(string)tournament["Index"]}.json"));
    }

    if (data == null)
        throw new Exception("Must have data by now");

    int i = 0;
    var golfers = (JArray)data["Golfers"];
    var poolies = data["Poolies"].OrderByDescending(x => (double)x["YtdPoints"]).ToList();
    foreach (JObject poolie in poolies)
    {
        if (i == 0)
            poolie["Rank"] = i + 1;
        else
        {
            poolie["Rank"] = (double)poolies[i - 1]["YtdPoints"] > (double)poolie["YtdPoints"] ? i + 1 : (int)poolies[i - 1]["Rank"];
        }
        i++;
    }

    i = 0;
    poolies = data["Poolies"].OrderByDescending(x => (double)x["YtdPoints"] + (double)x["Points"]).ToList();
    foreach (JObject poolie in poolies)
    {
        if (i == 0)
            poolie["ProjectedRank"] = i + 1;
        else
        {
            var prev = (double)poolies[i - 1]["YtdPoints"] + (double)poolies[i - 1]["Points"];
            var now = (double)poolie["YtdPoints"] + (double)poolie["Points"];
            poolie["ProjectedRank"] = prev > now ? i + 1 : (int)poolies[i - 1]["ProjectedRank"];
        }
        i++;

        var nameSplit = ((string)poolie["Name"]).Split(new[] { ' ' });
        poolie["LastFirst"] = nameSplit.Last() + ", " + nameSplit.First();
        var golfer = golfers.SingleOrDefault(x => (string)poolie["Golfer"] == (string)x["Id"]);
        if(golfer != null)
        {
            poolie["GolferName"] = (string)golfer["Name"];
            poolie["GolferRank"] = (string)golfer["Rank"];
            poolie["GolferTied"] = (string)golfer["Tied"];
        }
    }
    data["Poolies"] = JArray.FromObject(poolies.OrderBy(x => (int)x["ProjectedRank"]));

    return req.CreateOk(data);
}
