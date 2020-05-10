﻿/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using IQFeed.CSharpApiClient;
using IQFeed.CSharpApiClient.Lookup;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.ToolBox.IQFeed;
using QuantConnect.Util;

namespace QuantConnect.ToolBox.IQFeedDownloader
{
    /// <summary>
    /// IQFeed Downloader Toolbox Project For LEAN Algorithmic Trading Engine.
    /// </summary>
    public static class IQFeedDownloaderProgram
    {
        private const int NumberOfClients = 8;

        /// <summary>
        /// Primary entry point to the program. This program only supports EQUITY for now.
        /// </summary>
        public static void IQFeedDownloader(IList<string> tickers, string resolution, DateTime fromDate, DateTime toDate)
        {
            if (resolution.IsNullOrEmpty() || tickers.IsNullOrEmpty())
            {
                Console.WriteLine("IQFeedDownloader ERROR: '--tickers=' or '--resolution=' parameter is missing");
                Console.WriteLine("--tickers=SPY,AAPL");
                Console.WriteLine("--resolution=Tick/Second/Minute/Hour/Daily/All");
                Environment.Exit(1);
            }
            try
            {
                // Load settings from command line
                var allResolution = resolution.ToLowerInvariant() == "all";
                var castResolution = allResolution ? Resolution.Tick : (Resolution)Enum.Parse(typeof(Resolution), resolution);
                var startDate = fromDate.ConvertToUtc(TimeZones.NewYork);
                var endDate = toDate.ConvertToUtc(TimeZones.NewYork);
                endDate = endDate.AddDays(1).AddMilliseconds(-1);

                // Load settings from config.json
                var dataDirectory = Config.Get("data-folder", "../../../Data");
                var userName = Config.Get("iqfeed-username", "username");
                var password = Config.Get("iqfeed-password", "password");
                var productName = Config.Get("iqfeed-productName", "productname");
                var productVersion = Config.Get("iqfeed-version", "productversion");

                // Create an instance of the downloader
                var market = Market.USA;

                // Connect to IQFeed
                IQFeedLauncher.Start(userName, password, productName, productVersion);
                var lookupClient = LookupClientFactory.CreateNew(NumberOfClients);

                switch (tickers.First())
                {
                    case "CANADIAN_STOCKS":
                        market = Market.CANADA;
                        tickers = lookupClient.Symbol.GetAllMarketSymbols().Where(x => x.SecurityType == "EQUITY" && x.ListedMarket == "TSE")
                            .Select(x => x.Symbol).ToList();
                        break;

                    case "US_STOCKS":
                        tickers = lookupClient.Symbol.GetAllMarketSymbols().Where(x => x.SecurityType == "EQUITY" && new [] { "NYSE", "NASDAQ", "NYSE_AMERICAN", "BATS", "IEX" }.Contains(x.Exchange))
                            .Select(x => x.Symbol).ToList();
                        break;
                }

                lookupClient.Connect();

                // Create IQFeed downloader instance
                var universeProvider = new IQFeedDataQueueUniverseProvider();
                var historyProvider = new IQFeedFileHistoryProvider(lookupClient, universeProvider);
                var downloader = new IQFeedDataDownloader(historyProvider);

                var resolutions = allResolution ? new List<Resolution> { Resolution.Tick, Resolution.Second, Resolution.Minute, Resolution.Hour, Resolution.Daily } : new List<Resolution> { castResolution };
                var requests = resolutions.SelectMany(r => tickers.Select(t => new { Ticker = t, Resolution = r })).ToList();

                var sw = Stopwatch.StartNew();
                Parallel.ForEach(requests, new ParallelOptions { MaxDegreeOfParallelism = NumberOfClients }, request =>
                 {
                     // Download the data
                     var symbol = Symbol.Create(request.Ticker, SecurityType.Equity, market);
                     var data = downloader.Get(symbol, request.Resolution, startDate, endDate);

                     // Write the data
                     var writer = new LeanDataWriter(request.Resolution, symbol, dataDirectory);
                     writer.Write(data);
                 });
                sw.Stop();

                Log.Trace($"IQFeedDownloader: Completed successfully in {sw.Elapsed}!");
            }
            catch (Exception err)
            {
                Log.Error(err);
            }
        }
    }
}
