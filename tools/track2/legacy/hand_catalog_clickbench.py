#!/usr/bin/env python3
"""Retired hand-transcribed ClickBench scan catalogue. Evidence, not an input.

Same status as hand_catalog_tpch.py: geometry, column facts and the action
space are measured now, and this file exists only so `workload_snapshot.py
--compare clickbench` has something to check the runtime-derived catalogue
against. Frozen; do not extend.

The 43 queries are the official Spark suite, which E2 and E8 both run in full
-- the published protocol is the acceptance gate. Search may compress
internally (DB2 6.2) but may not drop a query from acceptance.
Texts: tools/track2/clickbench_queries.json.

COLUMN_ORDER survives only because the transcription of Q24 (`SELECT *`)
refers to it.
"""

from __future__ import annotations

D = lambda s: s


COLUMN_ORDER = {
    "hits": [
        "WatchID", "JavaEnable", "Title", "GoodEvent", "EventTime", "EventDate",
        "CounterID", "ClientIP", "RegionID", "UserID", "CounterClass", "OS",
        "UserAgent", "URL", "Referer", "IsRefresh", "RefererCategoryID",
        "RefererRegionID", "URLCategoryID", "URLRegionID", "ResolutionWidth",
        "ResolutionHeight", "ResolutionDepth", "FlashMajor", "FlashMinor",
        "FlashMinor2", "NetMajor", "NetMinor", "UserAgentMajor",
        "UserAgentMinor", "CookieEnable", "JavascriptEnable", "IsMobile",
        "MobilePhone", "MobilePhoneModel", "Params", "IPNetworkID",
        "TraficSourceID", "SearchEngineID", "SearchPhrase", "AdvEngineID",
        "IsArtifical", "WindowClientWidth", "WindowClientHeight",
        "ClientTimeZone", "ClientEventTime", "SilverlightVersion1",
        "SilverlightVersion2", "SilverlightVersion3", "SilverlightVersion4",
        "PageCharset", "CodeVersion", "IsLink", "IsDownload", "IsNotBounce",
        "FUniqID", "OriginalURL", "HID", "IsOldCounter", "IsEvent",
        "IsParameter", "DontCountHits", "WithHash", "HitColor",
        "LocalEventTime", "Age", "Sex", "Income", "Interests", "Robotness",
        "RemoteIP", "WindowName", "OpenerName", "HistoryLength",
        "BrowserLanguage", "BrowserCountry", "SocialNetwork", "SocialAction",
        "HTTPError", "SendTiming", "DNSTiming", "ConnectTiming",
        "ResponseStartTiming", "ResponseEndTiming", "FetchTiming",
        "SocialSourceNetworkID", "SocialSourcePage", "ParamPrice",
        "ParamOrderID", "ParamCurrency", "ParamCurrencyID",
        "OpenstatServiceName", "OpenstatCampaignID", "OpenstatAdID",
        "OpenstatSourceID", "UTMSource", "UTMMedium", "UTMCampaign",
        "UTMContent", "UTMTerm", "FromTag", "HasGCLID", "RefererHash",
        "URLHash", "CLID",
    ],
}

def P(column, op, value, value2=None):
    rec = {"column": column, "op": op, "value": value}
    if value2 is not None:
        rec["value2"] = value2
    return rec


def S(table, columns, predicates=None):
    return {"table": table, "columns": list(columns), "predicates": list(predicates or [])}


# Official ClickBench Spark suite (43 queries). E2/E8 use all of them,
# same rule as TPC-H 22: the published protocol is the gate. Search may
# compress internally (DB2 §6.2) but cannot drop a query from acceptance.
# Texts: tools/track2/clickbench_queries.json.
_HITS = COLUMN_ORDER["hits"]
_JULY = [
    P("EventDate", "ge", D("2013-07-01")),
    P("EventDate", "le", D("2013-07-31")),
]
_C62 = [P("CounterID", "eq", 62)]

QUERIES = {
    1: [S("hits", [])],
    2: [S("hits", ["AdvEngineID"], [P("AdvEngineID", "ne", 0)])],
    3: [S("hits", ["AdvEngineID", "ResolutionWidth"])],
    4: [S("hits", ["UserID"])],
    5: [S("hits", ["UserID"])],
    6: [S("hits", ["SearchPhrase"])],
    7: [S("hits", ["EventDate"])],
    8: [S("hits", ["AdvEngineID"], [P("AdvEngineID", "ne", 0)])],
    9: [S("hits", ["RegionID", "UserID"])],
    10: [S("hits", ["RegionID", "AdvEngineID", "ResolutionWidth", "UserID"])],
    11: [S("hits", ["MobilePhoneModel", "UserID"],
           [P("MobilePhoneModel", "ne", "")])],
    12: [S("hits", ["MobilePhone", "MobilePhoneModel", "UserID"],
           [P("MobilePhoneModel", "ne", "")])],
    13: [S("hits", ["SearchPhrase"], [P("SearchPhrase", "ne", "")])],
    14: [S("hits", ["SearchPhrase", "UserID"], [P("SearchPhrase", "ne", "")])],
    15: [S("hits", ["SearchEngineID", "SearchPhrase"],
           [P("SearchPhrase", "ne", "")])],
    16: [S("hits", ["UserID"])],
    17: [S("hits", ["UserID", "SearchPhrase"])],
    18: [S("hits", ["UserID", "SearchPhrase"])],
    19: [S("hits", ["UserID", "EventTime", "SearchPhrase"])],
    20: [S("hits", ["UserID"], [P("UserID", "eq", 435090932899640449)])],
    21: [S("hits", ["URL"], [P("URL", "like", "%google%")])],
    22: [S("hits", ["SearchPhrase", "URL"],
           [P("URL", "like", "%google%"), P("SearchPhrase", "ne", "")])],
    23: [S("hits", ["SearchPhrase", "URL", "Title", "UserID"],
           [P("Title", "like", "%Google%"), P("SearchPhrase", "ne", "")])],
    24: [S("hits", _HITS, [P("URL", "like", "%google%")])],
    25: [S("hits", ["SearchPhrase", "EventTime"], [P("SearchPhrase", "ne", "")])],
    26: [S("hits", ["SearchPhrase"], [P("SearchPhrase", "ne", "")])],
    27: [S("hits", ["SearchPhrase", "EventTime"], [P("SearchPhrase", "ne", "")])],
    28: [S("hits", ["CounterID", "URL"], [P("URL", "ne", "")])],
    29: [S("hits", ["Referer"], [P("Referer", "ne", "")])],
    30: [S("hits", ["ResolutionWidth"])],
    31: [S("hits", ["SearchEngineID", "ClientIP", "IsRefresh", "ResolutionWidth",
                    "SearchPhrase"],
           [P("SearchPhrase", "ne", "")])],
    32: [S("hits", ["WatchID", "ClientIP", "IsRefresh", "ResolutionWidth",
                    "SearchPhrase"],
           [P("SearchPhrase", "ne", "")])],
    33: [S("hits", ["WatchID", "ClientIP", "IsRefresh", "ResolutionWidth"])],
    34: [S("hits", ["URL"])],
    35: [S("hits", ["URL"])],
    36: [S("hits", ["ClientIP"])],
    37: [S("hits",
           ["URL", "CounterID", "EventDate", "DontCountHits", "IsRefresh"],
           _C62 + _JULY + [P("DontCountHits", "eq", 0), P("IsRefresh", "eq", 0),
                           P("URL", "ne", "")])],
    38: [S("hits",
           ["Title", "CounterID", "EventDate", "DontCountHits", "IsRefresh"],
           _C62 + _JULY + [P("DontCountHits", "eq", 0), P("IsRefresh", "eq", 0),
                           P("Title", "ne", "")])],
    39: [S("hits",
           ["URL", "CounterID", "EventDate", "IsRefresh", "IsLink", "IsDownload"],
           _C62 + _JULY + [P("IsRefresh", "eq", 0), P("IsLink", "ne", 0),
                           P("IsDownload", "eq", 0)])],
    40: [S("hits",
           ["TraficSourceID", "SearchEngineID", "AdvEngineID", "Referer", "URL",
            "CounterID", "EventDate", "IsRefresh"],
           _C62 + _JULY + [P("IsRefresh", "eq", 0)])],
    41: [S("hits",
           ["URLHash", "EventDate", "CounterID", "IsRefresh", "TraficSourceID",
            "RefererHash"],
           _C62 + _JULY + [P("IsRefresh", "eq", 0),
                           P("TraficSourceID", "in", [-1, 6]),
                           P("RefererHash", "eq", 3594120000172545465)])],
    42: [S("hits",
           ["WindowClientWidth", "WindowClientHeight", "CounterID", "EventDate",
            "IsRefresh", "DontCountHits", "URLHash"],
           _C62 + _JULY + [P("IsRefresh", "eq", 0), P("DontCountHits", "eq", 0),
                           P("URLHash", "eq", 2868770270353813622)])],
    43: [S("hits",
           ["EventTime", "CounterID", "EventDate", "IsRefresh", "DontCountHits"],
           _C62 + [P("EventDate", "ge", D("2013-07-14")),
                   P("EventDate", "le", D("2013-07-15")),
                   P("IsRefresh", "eq", 0), P("DontCountHits", "eq", 0)])],
}
