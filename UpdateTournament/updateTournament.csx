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

public static async Task Run(TimerInfo timer, TraceWriter log)
{
    var start = DateTime.UtcNow;
    var connectionString = "StatsStorage".GetEnvVar();
    var blobService = new BlobService(connectionString);

    var tournamentsUrl = "TournamentsUrl".GetEnvVar();
    var seasonData = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/Season", null, "ServiceToken".GetEnvVar());
    var season = (int)seasonData["Season"];
    var week = (int)seasonData["CurrentWeek"];

    // We only need previous or current tournaments. Not picking either because nothing has happened.
    var tournaments = await RestService.AuthorizedPostAsync($"{tournamentsUrl}/api/GetTournaments", new Dictionary<string, string>
    {
        ["season"] = "current",
        ["tour"] = "PGA TOUR",
        ["key"] = "state",
        ["value"] = "dequeued,progressing,completed",
    }, "ServiceToken".GetEnvVar());

    // we need to Max Week value so that we know when the week number wraps around:
    var maxWeek = tournaments.Select(x => (int)x["WeekNumber"]).Max();

    // Set tournaments to "calc" if they need to be calculated.
    // Dequeued tournaments only need to be calced if they don't have stats.
    foreach (JObject tournament in tournaments)
    {
        if (!await blobService.BlobExists("tournament", $"{season}/{(string)tournament["Tour"]}/{(string)tournament["Index"]}.json"))
            tournament["calc"] = true;
        else if ((bool)tournament["Used"] == true)
            tournament["calc"] = true;
    }

    List<JObject> profiles = await GetProfiles();

    foreach (JObject tournament in tournaments)
    {
        if (tournament["calc"] == null)
            continue;

        var fecPointTolerance = CalculateFecCupTolerance(tournament);
        log.Info($"Time tolerance: {fecPointTolerance}");
        XDocument xFecPoints = await RefreshFileService.RefreshXmlFile(connectionString, "data", $"r/{(string)tournament["PermanentNumber"]}/fecpoints.xml", fecPointTolerance);
        if(xFecPoints == null)
        {
            if ((string)tournament["State"] == "progressing")
            {
                xFecPoints = await RefreshFileService.RefreshXmlFile(connectionString, "data", $"r/current/fecpoints.xml", fecPointTolerance);
                await blobService.UploadBlobAsync("data", $"r/{(string)tournament["PermanentNumber"]}/fecpoints.xml", xFecPoints.ToString());
            }
            else
            {
                if(xFecPoints == null) // if it's STILL null
                {
                    var xFecPointsString = await blobService.DownloadBlobAsync("data", $"r/{(string)tournament["PermanentNumber"]}/fecpoints.xml");
                    xFecPoints = XDocument.Parse(xFecPointsString);
                }
            }
            if(xFecPoints == null)
            {
                throw new Exception("unable to get FecPoints.xml file.");
            }
        }
        XDocument xPlayerStats = await RefreshFileService.RefreshXmlFile(connectionString, "data", $"r/{(string)tournament["PermanentNumber"]}/player_stats.xml", fecPointTolerance);
        if(xPlayerStats == null)
        {
            if ((string)tournament["State"] == "progressing")
            {
                xPlayerStats = await RefreshFileService.RefreshXmlFile(connectionString, "data", $"r/current/player_stats.xml", fecPointTolerance);
                await blobService.UploadBlobAsync("data", $"r/{(string)tournament["PermanentNumber"]}/player_stats.xml", xPlayerStats.ToString());
            }
            else
            {
                if (xPlayerStats == null) // if it's STILL null
                {
                    var xPlayerStatsString = await blobService.DownloadBlobAsync("data", $"r/{(string)tournament["PermanentNumber"]}/player_stats.xml");
                    xPlayerStats = XDocument.Parse(xPlayerStatsString);
                }
            }
            if (xPlayerStats == null)
            {
                throw new Exception("unable to get player_stats.xml file.");
            }
        }

        string fedExPointsDistString = await blobService.DownloadBlobAsync("data", "pool/fedexPointsDist.json");
        JObject fedExPointsDist = JObject.Parse(fedExPointsDistString);

        string customPointsString = await blobService.DownloadBlobAsync("data", "pool/customPoints.json");
        JObject customPoints = JObject.Parse(customPointsString);

        JObject tournamentStat = CreateTournamentStat(tournament, xFecPoints, season, "PGA TOUR");
        List<JObject> picks = await GetPicks((string)tournament["Index"]);

        JArray previousPoolies = new JArray();
        var previousTournamentIndex = PreviousTournamentIndex((JArray)tournaments, (int)tournament["WeekNumber"], maxWeek);
        if(!string.IsNullOrEmpty(previousTournamentIndex))
        {
            var previousTournament = await blobService.DownloadBlobAsync("tournament", $"{season}/{(string)tournament["Tour"]}/{previousTournamentIndex}.json");
            var jPrev = JObject.Parse(previousTournament);
            previousPoolies = jPrev["Poolies"] as JArray;
        }
       

        List<string> golferIds = new List<string>();
        foreach (JObject pick in picks)
        {
            if (!golferIds.Contains((string)pick["PlayerId"]))
            {
                var golfer = GenerateGolfer(pick, tournament, xFecPoints, xPlayerStats, fedExPointsDist, customPoints);
                if(golfer == null)
                {
                    golfer = JObject.FromObject(new
                    {
                        Name = (string)pick["PlayerName"],
                        Id = (string)pick["PlayerId"],
                        PickCount = 1,
                        Rank = 0,
                        Tied = false,
                        IsMember = false,
                        Status = "active",
                    });
                }
                (tournamentStat["Golfers"] as JArray).Add(golfer);
                golferIds.Add((string)pick["PlayerId"]);
            }
            else
            {
                var existingGolfer = (JObject)((JArray)tournamentStat["Golfers"]).SingleOrDefault(x => (string)x["Id"] == (string)pick["PlayerId"]);
                var pickCount = (int)existingGolfer["PickCount"];
                existingGolfer["PickCount"] = ((int)existingGolfer["PickCount"]) + 1;
            }
        }

        foreach(JObject golfer in (tournamentStat["Golfers"] as JArray))
        {
            if(golfer["Points"] == null)
            {
                var golferRank = (int)golfer["Rank"];
                if(golferRank != 0)
                {
                    golfer["Points"] = 0;
                    if((bool)golfer["Tied"] != true)
                    {
                        var points = GetPointsForTournamentRank((string)tournament["TournamentType"], (int)golfer["Rank"], fedExPointsDist);
                        golfer["Points"] = points;
                    }
                    else
                    {
                        XElement standings = xFecPoints.XPathSelectElement("trn/standings");
                        foreach(XElement plrElement in standings.Elements("plr"))
                        {
                            if(plrElement.Element("event") != null)
                            {
                                XElement eventElement = plrElement.Element("event");
                                if(eventElement.Attribute("cPos") != null)
                                {
                                    var cpos = GetBackupString(plrElement, ".//event", "cPos", "0");
                                    if(Convert.ToInt32(cpos) == golferRank)
                                    {
                                        var pointsString = GetBackupString(plrElement, ".//points", "event", "0");
                                        double points = 0.00d;
                                        try
                                        {
                                            points = Convert.ToDouble(pointsString);
                                        }
                                        catch
                                        {
                                            points = 0;
                                        }
                                        golfer["Points"] = points;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        foreach (JObject custom in (JArray)customPoints[season.ToString()])
        {
            if ((string)custom["TournamentId"] == (string)tournament["PermanentNumber"])
            {
                foreach (JObject golfer in (tournamentStat["Golfers"] as JArray))
                {
                    string golferId = (string)golfer["Id"];
                    string customId = (string)custom["PlayerId"];
                    if (golferId == customId)
                    {
                        golfer["Points"] = (double)custom["Points"];
                        golfer["Rank"] = (int)custom["Rank"];
                    }
                }
            }
        }

        if(picks.Count > 0)
        {
            var golfers = ((JArray)tournamentStat["Golfers"]).ToObject<List<JObject>>().OrderByDescending(x => (int)x["PickCount"]).ToList();
            tournamentStat["Golfers"] = JArray.FromObject(golfers);
        }

        foreach (dynamic profile in profiles)
        {
            if (profile.isTest != null)
                continue;
            var pick = picks.FirstOrDefault(x => (string)x["UserId"] == (string)profile.UserId);
            var userId = profile.UserId;
            var email = profile.Email;
            var name = profile.Name;
            var golfer = pick != null ? (string)pick["PlayerId"] : "";
            var golferObject = pick != null ? tournamentStat["Golfers"].FirstOrDefault(x => (string)x["Id"] == (string)golfer) : null;
            double points = 0.0;
            if(golferObject != null && golferObject["Points"] != null)
                points = golferObject != null ? (double)golferObject["Points"] : 0.0d;
            double ytdPoints = 0.0d;

            var prePoolie = previousPoolies.FirstOrDefault(x => (string)x["UserId"] == (string)userId);
            if (prePoolie != null)
            {
                var prePoints = (double)prePoolie["Points"];
                var preYtd = (double)prePoolie["YtdPoints"];
                ytdPoints = (double)prePoints + (double)preYtd;
            }

            ((JArray)tournamentStat["Poolies"]).Add(JObject.FromObject(new
            {
                Name = name,
                Email = email,
                UserId = userId,
                Golfer = golfer,
                Points = points,
                YtdPoints = ytdPoints,
            }));
        }

        var poolies = ((JArray)tournamentStat["Poolies"]).ToObject<List<JObject>>().Where(x => x["isTest"] == null).OrderByDescending(x => (double)x["Points"] + (double)x["YtdPoints"]).ToList();
        tournamentStat["Poolies"] = JArray.FromObject(poolies);

        var data = RankStats(tournamentStat);

        await blobService.UploadBlobAsync("tournament", $"{season}/{(string)tournament["Tour"]}/{(string)tournament["Index"]}.json", data.ToString(Formatting.Indented));
    }

    var end = DateTime.UtcNow;
    log.Info($"Execution time: {end - start}");
}

public static JObject RankStats(JObject data)
{
    // build ranks
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
        if (golfer != null)
        {
            poolie["GolferName"] = (string)golfer["Name"];
            poolie["GolferRank"] = (int)golfer["Rank"];
            poolie["GolferTied"] = (string)golfer["Tied"];
        }
    }
    data["Poolies"] = JArray.FromObject(poolies.OrderBy(x => (int)x["ProjectedRank"]));
    return data;
}

public static TimeSpan CalculateFecCupTolerance(JObject tournament)
{
    var state = (string)tournament["State"];
    if (state == "dequeued")
        return TimeSpan.FromHours(12);
    if(state == "progressing")
    {
        var now = DateTime.UtcNow;
        var isThursday = (now.DayOfWeek == DayOfWeek.Thursday && now.Hour > 15) || (now.DayOfWeek == DayOfWeek.Friday && now.Hour < 6);
        var isFriday = (now.DayOfWeek == DayOfWeek.Friday && now.Hour > 15) || (now.DayOfWeek == DayOfWeek.Saturday && now.Hour < 6);
        var isSaturday = (now.DayOfWeek == DayOfWeek.Saturday && now.Hour > 15) || (now.DayOfWeek == DayOfWeek.Sunday && now.Hour < 6);
        var isSunday = (now.DayOfWeek == DayOfWeek.Sunday && now.Hour > 15) || (now.DayOfWeek == DayOfWeek.Monday && now.Hour < 6);
        if (isThursday || isFriday || isSaturday || isSunday)
            return TimeSpan.FromMinutes(10);
        return TimeSpan.FromHours(1);
    }
    if(state == "completed")
    {
        return TimeSpan.FromHours(2);
    }
    return TimeSpan.FromHours(12);
}

public static JObject CreateTournamentStat(dynamic tournament, XDocument xFecPoints, int season, string tour)
{
    XElement trn = xFecPoints.XPathSelectElement("trn");
    string strTotalPoints = GetBackupString(trn, ".//event/cup", "totalPoints", "0");
    string strWinnerShare = GetBackupString(trn, ".//event/cup", "winnerShare", "0");
    string strTotalRounds = GetBackupString(trn, ".//event", "totalRnds", "0");
    string strCurrentRound = GetBackupString(trn, ".//event", "currentRnd", "0");
    string strFieldSize = GetBackupString(trn, ".//event", "fieldSize", "0");

    double totalPoints = Convert.ToDouble(strTotalPoints);
    double winnerShare = Convert.ToDouble(strWinnerShare);
    int totalRounds = Convert.ToInt32(strTotalRounds);
    int currentRound = Convert.ToInt32(strCurrentRound);
    int fieldSize = Convert.ToInt32(strFieldSize);

    string name = tournament.Name;

    var stat = JObject.FromObject(new
    {
        Name = tournament.Name,
        Id = tournament.PermanentNumber,
        Index = tournament.Index,
        Tour = tour,
        Season = season,
        CurrentWeek = tournament.WeekNumber,
        Start = tournament.Start,
        End = tournament.End,
        TotalPoints = totalPoints,
        WinnerShare = winnerShare,
        IsPlayoff = tournament.IsPlayoff,
        IsMajor = tournament.IsMajor,
        TotalRounds = totalRounds,
        CurrentRound = currentRound,
        FieldSize = fieldSize,
        Course = tournament.CourseData,
        Champion = tournament.ChampionData,
        State = tournament.State,
        Golfers = new JArray(),
        Poolies = new JArray(),
    });

    return stat;
}

public static string GetBackupString(XElement xElement, string element, string attribute, string backup)
{
    if (xElement.XPathSelectElement($".//{element}") == null)
        return backup;
    string str = xElement.XPathSelectElement($".//{element}").Attribute($"{attribute}").Value;
    if (string.IsNullOrEmpty(str))
        str = backup;
    return str;
}

public static async Task<List<JObject>> GetPicks(string tournamentIndex)
{
    var picksUrl = "PicksUrl".GetEnvVar();
    List<JObject> list = new List<JObject>();
    JArray picks = (JArray)(await RestService.AuthorizedPostAsync($"{picksUrl}/api/GetPicks", new Dictionary<string, string>
    {
        ["season"] = "current",
        ["tour"] = "PGA TOUR",
        ["tournamentIndex"] = tournamentIndex,
    }, "ServiceToken".GetEnvVar()));

    foreach (var item in picks)
    {
        list.Add((JObject)item);
    }
    return list;
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

public static JObject GenerateGolfer(dynamic pick, dynamic tournament, XDocument xFecPoints, XDocument xPlayerStats, JObject fedExPointsDist, JObject customRanks)
{
    XElement standings = xFecPoints.XPathSelectElement("trn/standings");
    XElement player = standings.XPathSelectElement($".//plr[@id='{pick.PlayerId}']");
    if (player == null)
    {
        JObject nonMember = GenerateNonMember(pick, xFecPoints, xPlayerStats);
        if (nonMember == null)
        {
            nonMember = JObject.FromObject(new
            {
                Name = (string)pick["PlayerName"],
                Id = (string)pick["PlayerId"],
                PickCount = 1,
                Rank = 0,
                Tied = false,
                IsMember = false,
                Status = "active"
            });
        }
        return nonMember;    
    }
    string strPoints = GetBackupString(player, ".//points", "event", "0");
    string strMoney = GetBackupString(player, ".//money", "event", "0");
    string strRank = GetBackupString(player, ".//event", "cPos", "0");
    string strStatus = GetBackupString(player, ".//event", "status", "unknown");
    string strTied = GetBackupString(player, ".//event", "cPosTied", "x");
    string strRound = GetBackupString(player, ".//event", "rnd", "0");
    string strThru = GetBackupString(player, ".//event", "thru", "0");
    string strParTotal = GetBackupString(player, ".//event", "pTot", "0");
    string strParDay = GetBackupString(player, ".//event", "pDay", "0");
    string strScore = GetBackupString(player, ".//event", "sc", "0");

    double points = Convert.ToDouble(strPoints);
    double money = Convert.ToDouble(strMoney);
    int rank = Convert.ToInt32(strRank);
    
    bool tied = strTied.ToLower().StartsWith("y");
    int round = Convert.ToInt32(strRound);
    int thru = Convert.ToInt32(strThru);
    int parTotal = 0;
    try
    {
        parTotal = Convert.ToInt32(strParTotal);
    }
    catch
    {
        parTotal = 0;
    }

    int parDay = 0;
    try
    {
        parDay = Convert.ToInt32(strParDay);
    }
    catch
    {
        parDay = 0;
    }

    int score = Convert.ToInt32(strScore);

    var golfer = JObject.FromObject(new
    {
        Name = pick.PlayerName,
        Id = pick.PlayerId,
        PickCount = 1,
        Points = points,
        Money = money,
        Status = strStatus,
        Rank = rank,
        Tied = tied,
        Round = round,
        Thru = thru,
        ParTotal = parTotal,
        ParDay = parDay,
        Score = score,
        IsMember = true,
    });

    return golfer;
}

public static JObject GenerateNonMember(dynamic pick, XDocument xFecPoints, XDocument xPlayerStats)
{
    XElement player = xPlayerStats.XPathSelectElement($"Players/Player[@PID='{pick.PlayerId}']");
    if (player == null)
        return null;
    string strRank = GetBackupString(player, ".//Stat[@StatId='02675']", "RankAll", "0");

    bool tied = strRank.Contains("T");
    if (strRank.Contains("T"))
    {
        strRank = strRank.Replace("T", "");
    }
    int rank = Convert.ToInt32(strRank);

    var golfer = JObject.FromObject(new
    {
        Name = pick.PlayerName,
        Id = pick.PlayerId,
        PickCount = 1,
        Rank = rank,
        Tied = tied,
        IsMember = false,
        Status = "active"
    });
    return golfer;
}

public static string PreviousTournamentIndex(JArray tournaments, int currentWeek, int maxWeek)
{
    var previousWeek = currentWeek - 1;
    if (previousWeek == 0) previousWeek = maxWeek;
    foreach (var tournament in tournaments)
    {
        if ((int)tournament["WeekNumber"] == previousWeek)
            return (string)tournament["Index"];
    }
    return string.Empty;
}

public static double GetPointsForTournamentRank(string tournamentType, int rank, JObject fedExPointsDist)
{
    var indexName = "";
    if(tournamentType == "WGC")
    {
        indexName = "WGC";
    }
    else if(tournamentType == "MJR")
    {
        indexName = "Majors";
    }
    else if(tournamentType == "LRS" || tournamentType == "STD")
    {
        indexName = "PGATOUR";
    }
    else if(tournamentType == "PLF" || tournamentType == "PLS")
    {
        indexName = "Playoffs";
    }

    if (string.IsNullOrEmpty(indexName))
    {
        return 0;
    }

    JArray points = (JArray)fedExPointsDist[indexName];
    return (double)points[rank - 1];
} 