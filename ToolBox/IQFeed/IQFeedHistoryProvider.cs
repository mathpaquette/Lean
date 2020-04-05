using System;
using System.Collections.Generic;
using System.IO;
using IQFeed.CSharpApiClient.Lookup;
using IQFeed.CSharpApiClient.Lookup.Historical.Enums;
using IQFeed.CSharpApiClient.Lookup.Historical.Messages;
using QuantConnect.Brokerages;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;

namespace QuantConnect.ToolBox.IQFeed
{
    public class IQFeedHistoryProvider
    {
        private readonly ISymbolMapper _symbolMapper;
        private readonly bool _saveOnDisk;
        private readonly LookupClient<double> _lookupClient;

        public IQFeedHistoryProvider(LookupClient<double> lookupClient, ISymbolMapper symbolMapper, bool saveOnDisk = false)
        {
            _lookupClient = lookupClient;
            _symbolMapper = symbolMapper;
            _saveOnDisk = saveOnDisk;
        }

        public IEnumerable<Slice> ProcessHistoryRequests(HistoryRequest request)
        {
            // skipping universe and canonical symbols
            if (!CanHandle(request.Symbol) ||
                (request.Symbol.ID.SecurityType == SecurityType.Option && request.Symbol.IsCanonical()) ||
                (request.Symbol.ID.SecurityType == SecurityType.Future && request.Symbol.IsCanonical()))
            {
                yield break;
            }

            var ticker = _symbolMapper.GetBrokerageSymbol(request.Symbol);
            var start = request.StartTimeUtc.ConvertFromUtc(TimeZones.NewYork);
            DateTime? end = request.EndTimeUtc.ConvertFromUtc(TimeZones.NewYork);

            // if we're within a minute of now, don't set the end time
            if (request.EndTimeUtc >= DateTime.UtcNow.AddMinutes(-1))
            {
                end = null;
            }

            var isEquity = request.Symbol.SecurityType == SecurityType.Equity;

            Log.Trace(
                $"HistoryPort.ProcessHistoryJob(): Submitting request: {request.Symbol.SecurityType.ToStringInvariant()}-{ticker}: " +
                $"{request.Resolution.ToStringInvariant()} {start.ToStringInvariant()}->{(end ?? DateTime.UtcNow.AddMinutes(-1)).ToStringInvariant()}"
            );

            if (_saveOnDisk)
            {
                foreach (var slice in GetDataFromFile(request, ticker, start, end, isEquity))
                    yield return slice;
            }
            else
            {
                foreach (var slice in GetDataFromMemory(request, ticker, start, end, isEquity))
                    yield return slice;
            }
        }

        private IEnumerable<Slice> GetDataFromFile(HistoryRequest request, string ticker, DateTime startDate, DateTime? endDate, bool isEquity)
        {
            string filename = null;

            switch (request.Resolution)
            {
                case Resolution.Tick:
                    filename = _lookupClient.Historical.File.GetHistoryTickTimeframeAsync(ticker, startDate, endDate, dataDirection: DataDirection.Oldest).SynchronouslyAwaitTaskResult();
                    var tickMessages = TickMessage.ParseFromFile(filename);
                    foreach (var slice in GetSlicesFromTickMessages(tickMessages, request, isEquity)) yield return slice;
                    break;

                case Resolution.Daily:
                    filename = _lookupClient.Historical.File.GetHistoryDailyTimeframeAsync(ticker, startDate, endDate, dataDirection: DataDirection.Oldest).SynchronouslyAwaitTaskResult();
                    var dailyMessages = DailyWeeklyMonthlyMessage.ParseFromFile(filename);
                    foreach (var slice in GetSlicesFromDailyMessages(dailyMessages, request, isEquity)) yield return slice;
                    break;

                default:
                    var interval = new Interval(GetPeriodType(request.Resolution), 1);
                    filename = _lookupClient.Historical.File.GetHistoryIntervalTimeframeAsync(ticker, interval.Seconds, startDate, endDate, dataDirection: DataDirection.Oldest).SynchronouslyAwaitTaskResult();
                    var intervalMessages = IntervalMessage.ParseFromFile(filename);
                    foreach (var slice in GetSlicesFromIntervalMessages(intervalMessages, request, isEquity)) yield return slice;
                    break;
            }

            if (string.IsNullOrEmpty(filename))
                yield break;

            File.Delete(filename);
        }

        private IEnumerable<Slice> GetDataFromMemory(HistoryRequest request, string ticker, DateTime startDate, DateTime? endDate, bool isEquity)
        {
            switch (request.Resolution)
            {
                case Resolution.Tick:
                    var tickMessages = _lookupClient.Historical.GetHistoryTickTimeframeAsync(ticker, startDate, endDate, dataDirection: DataDirection.Oldest).SynchronouslyAwaitTaskResult();
                    foreach (var slice in GetSlicesFromTickMessages(tickMessages, request, isEquity)) yield return slice;
                    break;

                case Resolution.Daily:
                    var dailyMessages = _lookupClient.Historical.GetHistoryDailyTimeframeAsync(ticker, startDate, endDate, dataDirection: DataDirection.Oldest).SynchronouslyAwaitTaskResult();
                    foreach (var slice in GetSlicesFromDailyMessages(dailyMessages, request, isEquity)) yield return slice;
                    break;

                default:
                    var interval = new Interval(GetPeriodType(request.Resolution), 1);
                    var intervalMessages = _lookupClient.Historical.GetHistoryIntervalTimeframeAsync(ticker, interval.Seconds, startDate, endDate, dataDirection: DataDirection.Oldest).SynchronouslyAwaitTaskResult();
                    foreach (var slice in GetSlicesFromIntervalMessages(intervalMessages, request, isEquity)) yield return slice;
                    break;
            }
        }

        private IEnumerable<Slice> GetSlicesFromIntervalMessages(IEnumerable<IntervalMessage<double>> intervalMessages, HistoryRequest request, bool isEquity)
        {
            foreach (var i in intervalMessages)
            {
                if (i.Timestamp == DateTime.MinValue) continue;
                var istartTime = i.Timestamp - request.Resolution.ToTimeSpan();
                if (!isEquity) istartTime = istartTime.ConvertTo(TimeZones.NewYork, TimeZones.EasternStandard);
                var tradeBar = new TradeBar(
                    istartTime,
                    request.Symbol,
                    (decimal)i.Open,
                    (decimal)i.High,
                    (decimal)i.Low,
                    (decimal)i.Close,
                    i.PeriodVolume
                );
                yield return new Slice(tradeBar.EndTime, new[] { tradeBar });
            }
        }

        private IEnumerable<Slice> GetSlicesFromDailyMessages(IEnumerable<DailyWeeklyMonthlyMessage<double>> dailyMessages, HistoryRequest request, bool isEquity)
        {
            foreach (var d in dailyMessages)
            {
                if (d.Timestamp == DateTime.MinValue) continue;
                var dstartTime = d.Timestamp.Date;
                if (!isEquity) dstartTime = dstartTime.ConvertTo(TimeZones.NewYork, TimeZones.EasternStandard);
                var tradeBar = new TradeBar(
                    dstartTime,
                    request.Symbol,
                    (decimal)d.Open,
                    (decimal)d.High,
                    (decimal)d.Low,
                    (decimal)d.Close,
                    d.PeriodVolume,
                    request.Resolution.ToTimeSpan()
                );
                yield return new Slice(tradeBar.EndTime, new[] { tradeBar });
            }
        }

        private IEnumerable<Slice> GetSlicesFromTickMessages(IEnumerable<TickMessage<double>> tickMessages, HistoryRequest request, bool isEquity)
        {
            foreach (var t in tickMessages)
            {
                var time = isEquity ? t.Timestamp : t.Timestamp.ConvertTo(TimeZones.NewYork, TimeZones.EasternStandard);
                var tick = new Tick(time, request.Symbol, (decimal)t.Last, (decimal)t.Bid, (decimal)t.Ask)
                { Quantity = t.LastSize };
                yield return new Slice(tick.EndTime, new[] { tick });
            }
        }

        /// <summary>
        /// Returns true if this data provide can handle the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol to be handled</param>
        /// <returns>True if this data provider can get data for the symbol, false otherwise</returns>
        private bool CanHandle(Symbol symbol)
        {
            var market = symbol.ID.Market;
            var securityType = symbol.ID.SecurityType;
            return
                (securityType == SecurityType.Equity && market == Market.USA) ||
                (securityType == SecurityType.Forex && market == Market.FXCM) ||
                (securityType == SecurityType.Option && market == Market.USA) ||
                (securityType == SecurityType.Future);
        }

        private static PeriodType GetPeriodType(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Second:
                    return PeriodType.Second;
                case Resolution.Minute:
                    return PeriodType.Minute;
                case Resolution.Hour:
                    return PeriodType.Hour;
                case Resolution.Tick:
                case Resolution.Daily:
                default:
                    throw new ArgumentOutOfRangeException("resolution", resolution, null);
            }
        }
    }
}