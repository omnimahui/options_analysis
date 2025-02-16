from openbb import obb
import pandas as pd
import requests
from datetime import datetime, timedelta, date
import os

obb.user.preferences.output_type = "dataframe"


tickers_delisted = [
    "BRK.B",
    "$VIX.X",
    "HPJ",
    "SGOC",
    "CIFS",
    "RYB",
    "AUO",
    "SORL",
    "HGSH",
    "CYOU",
    "GSUM",
    "PPDF",
    "JMEI",
    "CLDC",
    "YIN",
    "HMI",
    "CTRP",
    "CHU",
    "OSN",
    "HLG",
    "BORN",
    "GSH",
    "CHL",
    "REDU",
    "CALI",
    "CBPO",
    "VISN",
    "JASO",
    "CNR",
    "CNTF",
    "HNP",
    "ATV",
    "WBAI",
    "SSW",
    "SINO",
    "EHIC",
    "CHA",
    "PTR",
    "SINA",
    "ACH",
    "JOBS",
    "LFC",
    "SHI",
    "AMCN",
    "WUBA",
    "HX",
    "BITA",
    "SNP",
    "JMU",
    "SOGO",
    "JP",
    "SFUN",
    "DL",
    "JRJC",
    "CCCL",
    "BSTI",
    "XRF",
    "CADC",
    "YGE",
    "SYUT",
    "CEO",
    "XNY",
    "SPIL",
    "KGJI",
    "GHII",
    "CCIH",
    "CISG",
    "CCCR",
    "KANG",
    "ALN",
    "JFC",
    "KONE",
    "CBAK",
    "ONP",
    "BSPM",
    "SKYS",
    "NFEC",
    "ZX",
    "CNIT",
    "BF.B",
    "LKM",
    "STV",
    "CXDC",
    "KGJI",
    "LKM",
    "BSPM",
    "SVA",
    "FFHL",
    "CEA",
    "ZNH",
    "STG",
    "CNET",
    "OIIM",
    "QTT",
    "JT",
    "PME",
    "TEDU",
    "LEJU",
    "CO",
    "RENN",
    "BNSO",
]


def get_sp500():
    global tickers_delisted
    website = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    web_df = pd.read_html(website)[0]

    us_df = pd.DataFrame(columns=web_df.columns)
    us_list = ["RIVN", "ARM","NET","PINS","RBLX","TTD","APP","HOOD","RDDT","COIN","ABNB","SERV","SOUN","YINN"]
    us_df["Symbol"] = us_list
    us_df["Security"] = us_list
    us_df["GICS Sector"] = "Consumer Discretionary"
    us_df["GICS Sub-Industry"] = "Automobile Manufacturers"
    web_df = pd.concat([web_df, us_df], axis=0)

    # ETF symbol
    etf_df = pd.DataFrame(columns=web_df.columns)
    etf_list = [
        "SPY",
        "$VIX.X",
        "QQQ",
        "KRE",
        "OIH",
        "IWM",
        "XLU",
        "GLD",
        "XLV",
        "XLE",
        "DIA",
        "JETS",
        "EEM",
        "XLI",
        "XLB",
        "XLY",
        "USO",
        "XHB",
        "XLK",
        "HACK",
        "XLF",
        "IBB",
        "XBI",
        "XME",
        "SLX",
        "SMH",
        "IBIT",
    ]
    etf_df["Symbol"] = etf_list
    etf_df["Security"] = etf_list
    etf_df["GICS Sector"] = "ETF"
    etf_df["GICS Sub-Industry"] = "ETF"
    web_df = pd.concat([web_df, etf_df], axis=0)

    # Get china stocks
    # c_df = pd.read_html('https://www.tradesmax.com/component/k2/item/4480-chinese-stock')
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36"
    }
    url = "https://www.tradesmax.com/component/k2/item/4480-chinese-stock"
    r = requests.get(url, headers=headers)
    c_df = pd.read_html(r.content, thousands=None, decimal=",")

    c_tickers_list = c_df[0]["代码"].to_list()
    c_tickers_list.extend(["XPEV", "LI", "BEKE", "DIDIY", "ZH", "TME", "GOTU"])
    c_web_df = pd.DataFrame(columns=web_df.columns)
    c_web_df["Symbol"] = c_tickers_list
    c_web_df["Security"] = c_tickers_list
    c_web_df["GICS Sector"] = "China CDR"
    c_web_df["GICS Sub-Industry"] = "China CDR"

    web_df = pd.concat([web_df, c_web_df], axis=0)
    tickers_list = list(web_df["Symbol"])
    tickers_list = list(set(tickers_list) - set(tickers_delisted))
    tickers_list = tickers_list[:]
    # print('tickers_list',len(tickers_list))

    return tickers_list, web_df


tickers, _ = get_sp500()
df_total = pd.DataFrame()
import time
for ticker in tickers:
    time.sleep(2)
    try:
        df = obb.derivatives.options.chains(symbol=ticker, provider="yfinance")
        df_total = pd.concat([df_total, df], ignore_index=True)
    except Exception as ex:
        print(f"no option data for {ticker}, {ex}")
        continue

folder = os.path.dirname(os.path.realpath(__file__)) + os.sep + "options" + os.sep
if not os.path.exists(folder):
    os.mkdir(folder)
fname = f"{folder}{date.today()}.hdf"
df_total.astype(str).to_hdf(fname, key="options", mode="a", format="t")
