import asyncio
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import random
import tenacity
from sqlalchemy import create_engine
from .config import settings
from bs4 import BeautifulSoup
import urllib
import requests
import json

# SQLALCHEMY_WAREHOUSE_URI = 'mysql+pymysql://pj:#%Rfb_7)Y<6k3-TP"TY?e6Dv:J6K[;,X@18.181.48.71:3306/stock_api_warehouse'
mysql_engine = create_engine(settings.SQLALCHEMY_WAREHOUSE_URI, pool_pre_ping=True)

with open('proxies.txt', 'r') as f:
    proxies = f.read().split('\n')


cmkeys_mapping = {
    '籌碼K線2': ['mainstockimg', 'stockmainkline'], 
    '籌碼K線': ['tradersum', 'stockmainkline'],
    '基本資料': ['GetStockBasicInfo', 'f00026'],
    '營收盈餘': ['GetStockRevenueSurplus', 'f00029'],
    '轉投資': ['GetReinvestment', 'f00031'],
    '本益比': ['GetPERAndPBR', 'f00032'],
    '資產負債表': ['GetBalanceSheet', 'f00040'],
    '損益表': ['GetIncomeStatement', 'f00041'],
    '現金流量表': ['GetCashFlowStatement', 'f00042'],
    '財務比率': ['GetFinancialRatios', 'f00043'],
}


class config:
    data = 0


def get_cmkey(cmkeys):
    r = requests.get('https://www.cmoney.tw/finance/f00032.aspx?s=1101')
    soup = BeautifulSoup(r.content)
    for href in soup.find('nav', {'class': 'primary'}).find_all('a'):
        for item in cmkeys.keys():
            if href.text == item:
                cmkeys[item].append(urllib.parse.quote(href.get('cmkey')))
    return cmkeys


def get_cmoney_stock_id():
    sql = "SELECT * FROM CMoneyStockInfo"
    with mysql_engine.connect() as connection:
        result = connection.execute(sql).fetchall()
    return result


def insert_to_db(data, CMKey, data_type):
    if data_type == 'mainstockimg':
        sql = """
              INSERT INTO CMoneyKImage (CMKey, Date, OpenPrice, HighPrice, LowPrice, ClosePrice, SaleQty, PriceDifference, MagnitudeOfPrice, SalePrice, CLimit)
              VALUES ('""" + CMKey + """', %(Date)s, %(OpenPrice)s, %(HighPrice)s, %(LowPrice)s, %(ColsePrice)s, %(SaleQty)s, %(PriceDifference)s, %(MagnitudeOfPrice)s, %(SalePrice)s, %(Limit)s)
              ON DUPLICATE KEY UPDATE OpenPrice = OpenPrice, HighPrice = HighPrice, LowPrice = LowPrice, ClosePrice = ClosePrice, SaleQty = SaleQty, PriceDifference = PriceDifference, MagnitudeOfPrice = MagnitudeOfPrice, SalePrice = SalePrice, CLimit = CLimit;
              """
    elif data_type == 'GetStockBasicInfo':
        sql = """
              INSERT INTO CMoneyStockBasicInfo (CMKey, Address, Business, ChairmanOfTheBoard, CompanyName, Date, DateOfEstablishment, DomesticShare, EnglishAbbreviation, ExportShare, GeneralManager, Industry, ListingDate, MarketPrice, MonthlyRevenue, MonthlyRevenueYearGrowth, OtcDate, PBR, PER, PaidInCapital, Phone, SpokesPerson, SpokesPersonTitle, StockTransferAgency, StockTransferAgencyPhone, SubIndustry, TseOtc, URL, VisaCertifiedPublicAccountants)
              VALUES ('""" + CMKey + """', %(Address)s, %(Business)s, %(ChairmanOfTheBoard)s, %(CompanyName)s, %(Date)s, %(DateOfEstablishment)s, %(DomesticShare)s, %(EnglishAbbreviation)s, %(ExportShare)s, %(GeneralManager)s, %(Industry)s, %(ListingDate)s, %(MarketPrice)s, %(MonthlyRevenue)s, %(MonthlyRevenueYearGrowth)s, %(OtcDate)s, %(PBR)s, %(PER)s, %(PaidInCapital)s, %(Phone)s, %(SpokesPerson)s, %(SpokesPersonTitle)s, %(StockTransferAgency)s, %(StockTransferAgencyPhone)s, %(SubIndustry)s, %(TseOtc)s, %(URL)s, %(VisaCertifiedPublicAccountants)s)
              ON DUPLICATE KEY UPDATE Address = Address, Business = Business, ChairmanOfTheBoard = ChairmanOfTheBoard, CompanyName = CompanyName, Date = Date, DateOfEstablishment = DateOfEstablishment, DomesticShare = DomesticShare, EnglishAbbreviation = EnglishAbbreviation, ExportShare = ExportShare, GeneralManager = GeneralManager, Industry = Industry, ListingDate = ListingDate, MarketPrice = MarketPrice, MonthlyRevenue = MonthlyRevenue, MonthlyRevenueYearGrowth = MonthlyRevenueYearGrowth, OtcDate = OtcDate, PBR = PBR, PER = PER, PaidInCapital = PaidInCapital, Phone = Phone, SpokesPerson = SpokesPerson, SpokesPersonTitle = SpokesPersonTitle, StockTransferAgency = StockTransferAgency, StockTransferAgencyPhone = StockTransferAgencyPhone, SubIndustry = SubIndustry, TseOtc = TseOtc, URL = URL, VisaCertifiedPublicAccountants = VisaCertifiedPublicAccountants;
              """

    elif data_type == 'tradersum':
        sql = """
              INSERT INTO CMoneyTraderSum (CMKey, BuyerCount, Date, SellerCount, TraderCount, TraderSum)
              VALUES ('""" + CMKey + """', %(BuyerCount)s, %(Date)s, %(SellerCount)s, %(TraderCount)s, %(TraderSum)s)
              ON DUPLICATE KEY UPDATE BuyerCount = BuyerCount, SellerCount = SellerCount, TraderCount = TraderCount, TraderSum = TraderSum;
              """

    elif data_type == 'GetStockRevenueSurplus':
        sql = """
              INSERT INTO CMoneyStockRevenueSurplus (CMKey, AccumulatedConsolidatedRevenue, AccumulatedConsolidatedRevenueGrowth, AccumulatedConsolidatedRevenueM, AccumulatedEPS, AccumulatedRevenue, AccumulatedRevenueGrowth, AccumulatedRevenueM, AccumulatedSurplus, ClosePr, Date, MonthlyConsolidatedRevenue, MonthlyConsolidatedRevenueM, MonthlyConsolidatedRevenueMonthlyChange, MonthlyConsolidatedRevenueYearGrowth, MonthlyRevenue, MonthlyRevenueM, MonthlyRevenueMonthlyChange, MonthlyRevenueYearGrowth)
              VALUES ('""" + CMKey + """', %(AccumulatedConsolidatedRevenue)s, %(AccumulatedConsolidatedRevenueGrowth)s, %(AccumulatedConsolidatedRevenueM)s, %(AccumulatedEPS)s, %(AccumulatedRevenue)s, %(AccumulatedRevenueGrowth)s, %(AccumulatedRevenueM)s, %(AccumulatedSurplus)s, %(ClosePr)s, %(Date)s, %(MonthlyConsolidatedRevenue)s, %(MonthlyConsolidatedRevenueM)s, %(MonthlyConsolidatedRevenueMonthlyChange)s, %(MonthlyConsolidatedRevenueYearGrowth)s, %(MonthlyRevenue)s, %(MonthlyRevenueM)s, %(MonthlyRevenueMonthlyChange)s, %(MonthlyRevenueYearGrowth)s)
              ON DUPLICATE KEY UPDATE AccumulatedConsolidatedRevenue = AccumulatedConsolidatedRevenue, AccumulatedConsolidatedRevenueGrowth = AccumulatedConsolidatedRevenueGrowth, AccumulatedConsolidatedRevenueM = AccumulatedConsolidatedRevenueM, AccumulatedEPS = AccumulatedEPS, AccumulatedRevenue = AccumulatedRevenue, AccumulatedRevenueGrowth = AccumulatedRevenueGrowth, AccumulatedRevenueM = AccumulatedRevenueM, AccumulatedSurplus = AccumulatedSurplus, ClosePr = ClosePr, MonthlyConsolidatedRevenue = MonthlyConsolidatedRevenue, MonthlyConsolidatedRevenueM = MonthlyConsolidatedRevenueM, MonthlyConsolidatedRevenueMonthlyChange = MonthlyConsolidatedRevenueMonthlyChange, MonthlyConsolidatedRevenueYearGrowth = MonthlyConsolidatedRevenueYearGrowth, MonthlyRevenue = MonthlyRevenue, MonthlyRevenueM = MonthlyRevenueM, MonthlyRevenueMonthlyChange = MonthlyRevenueMonthlyChange, MonthlyRevenueYearGrowth = MonthlyRevenueYearGrowth;
              """

    elif data_type == 'GetReinvestment':
        sql = """
              INSERT INTO CMoneyReinvestment (CMKey, BookValue, Currency, EvaluationOfBasic, GainsAndLossesRecognizedInTheCurrentPeriod, NumberOfShares, OwnershipCosts, ReinvestmentName, SeasonDate, ShareholdingRatio)
              VALUES ('""" + CMKey + """', %(BookValue)s, %(Currency)s, %(EvaluationOfBasic)s, %(GainsAndLossesRecognizedInTheCurrentPeriod)s, %(NumberOfShares)s, %(OwnershipCosts)s, %(ReinvestmentName)s, %(SeasonDate)s, %(ShareholdingRatio)s)
              ON DUPLICATE KEY UPDATE BookValue = BookValue, Currency = Currency, EvaluationOfBasic = EvaluationOfBasic, GainsAndLossesRecognizedInTheCurrentPeriod = GainsAndLossesRecognizedInTheCurrentPeriod, NumberOfShares = NumberOfShares, OwnershipCosts = OwnershipCosts, ReinvestmentName = ReinvestmentName, ShareholdingRatio = ShareholdingRatio;
              """

    elif data_type == 'GetPERAndPBR':
        sql = """
              INSERT INTO CMoneyPERAndPBR (CMKey, ClosePr, Date, HighPr, LowPr, OpenPr, PBR, PER, PERByTWSE)
              VALUES ('""" + CMKey + """', %(ClosePr)s, %(Date)s, %(HighPr)s, %(LowPr)s, %(OpenPr)s, %(PBR)s, %(PER)s, %(PERByTWSE)s)
              ON DUPLICATE KEY UPDATE ClosePr = ClosePr, HighPr = HighPr, LowPr = LowPr, OpenPr = OpenPr, PBR = PBR, PER = PER, PERByTWSE = PERByTWSE;
              """

    elif data_type == 'GetBalanceSheet':
        sql = """
              INSERT INTO CMoneyBalanceSheet (CMKey, AccountsPayable, AccountsPayableRelatedParties, AccountsReceivableRelatedPartiesNet, AccumulatedDepreciation, BookValuePerShare, CapitalStock, CashAndCashEquivalents, CurrentAssets, CurrentLiabilities, DateRange, FixedAssets, Inventories, Land, LongTermInvestments, LongTermLiabilities, NotesPayable, NotesPayableRelatedParties, NotesReceivableNet, OtherAccountsReceivable, OtherAccountsReceivablePrRelatedParties, OtherAssets, OtherLiabilities, ShareholdersEquity, TotalAccountsAndNotesReceivable, TotalAccountsPayable, TotalAccountsReceivable, TotalAssets, TotalLiabilities)
              VALUES ('""" + CMKey + """', %(AccountsPayable)s, %(AccountsPayableRelatedParties)s, %(AccountsReceivableRelatedPartiesNet)s, %(AccumulatedDepreciation)s, %(BookValuePerShare)s, %(CapitalStock)s, %(CashAndCashEquivalents)s, %(CurrentAssets)s, %(CurrentLiabilities)s, %(DateRange)s, %(FixedAssets)s, %(Inventories)s, %(Land)s, %(LongTermInvestments)s, %(LongTermLiabilities)s, %(NotesPayable)s, %(NotesPayableRelatedParties)s, %(NotesReceivableNet)s, %(OtherAccountsReceivable)s, %(OtherAccountsReceivablePrRelatedParties)s, %(OtherAssets)s, %(OtherLiabilities)s, %(ShareholdersEquity)s, %(TotalAccountsAndNotesReceivable)s, %(TotalAccountsPayable)s, %(TotalAccountsReceivable)s, %(TotalAssets)s, %(TotalLiabilities)s)
              ON DUPLICATE KEY UPDATE AccountsPayable = AccountsPayable, AccountsPayableRelatedParties = AccountsPayableRelatedParties, AccountsReceivableRelatedPartiesNet = AccountsReceivableRelatedPartiesNet, AccumulatedDepreciation = AccumulatedDepreciation, BookValuePerShare = BookValuePerShare, CapitalStock = CapitalStock, CashAndCashEquivalents = CashAndCashEquivalents, CurrentAssets = CurrentAssets, CurrentLiabilities = CurrentLiabilities, FixedAssets = FixedAssets, Inventories = Inventories, Land = Land, LongTermInvestments = LongTermInvestments, LongTermLiabilities = LongTermLiabilities, NotesPayable = NotesPayable, NotesPayableRelatedParties = NotesPayableRelatedParties, NotesReceivableNet = NotesReceivableNet, OtherAccountsReceivable = OtherAccountsReceivable, OtherAccountsReceivablePrRelatedParties = OtherAccountsReceivablePrRelatedParties, OtherAssets = OtherAssets, OtherLiabilities = OtherLiabilities, ShareholdersEquity = ShareholdersEquity, TotalAccountsAndNotesReceivable = TotalAccountsAndNotesReceivable, TotalAccountsPayable = TotalAccountsPayable, TotalAccountsReceivable = TotalAccountsReceivable, TotalAssets = TotalAssets, TotalLiabilities = TotalLiabilities;
              """

    elif data_type == 'GetIncomeStatement':
        sql = """
              INSERT INTO CMoneyIncomeStatement (CMKey, DateRange, EPS, EarningsAfterTaxes, EarningsBeforeTaxes, GainsFromOperations, GrossIncomeFromOperations, NonOperatingExpenses, NonOperatingIncomes, OperatingCosts, OperatingExpenses, OperatingInconeNet)
              VALUES ('""" + CMKey + """', %(DateRange)s, %(EPS)s, %(EarningsAfterTaxes)s, %(EarningsBeforeTaxes)s, %(GainsFromOperations)s, %(GrossIncomeFromOperations)s, %(NonOperatingExpenses)s, %(NonOperatingIncomes)s, %(OperatingCosts)s, %(OperatingExpenses)s, %(OperatingInconeNet)s)
              ON DUPLICATE KEY UPDATE EPS = EPS, EarningsAfterTaxes = EarningsAfterTaxes, EarningsBeforeTaxes = EarningsBeforeTaxes, GainsFromOperations = GainsFromOperations, GrossIncomeFromOperations = GrossIncomeFromOperations, NonOperatingExpenses = NonOperatingExpenses, NonOperatingIncomes = NonOperatingIncomes, OperatingCosts = OperatingCosts, OperatingExpenses = OperatingExpenses, OperatingInconeNet = OperatingInconeNet;
              """

    elif data_type == 'GetCashFlowStatement':
        sql = """
              INSERT INTO CMoneyCashFlowStatement (CMKey, CashAndCashEquivalents, CashFlowFromFinancingActivities, CashFlowFromInvestingActivities, CashFlowFromOperatingActivities, DateRange, EarningsBeforeTaxes, FreeCashFlow, NetCashFlow)
              VALUES ('""" + CMKey + """', %(CashAndCashEquivalents)s, %(CashFlowFromFinancingActivities)s, %(CashFlowFromInvestingActivities)s, %(CashFlowFromOperatingActivities)s, %(DateRange)s, %(EarningsBeforeTaxes)s, %(FreeCashFlow)s, %(NetCashFlow)s)
              ON DUPLICATE KEY UPDATE CashAndCashEquivalents = CashAndCashEquivalents, CashFlowFromFinancingActivities = CashFlowFromFinancingActivities, CashFlowFromInvestingActivities = CashFlowFromInvestingActivities, CashFlowFromOperatingActivities = CashFlowFromOperatingActivities, EarningsBeforeTaxes = EarningsBeforeTaxes, FreeCashFlow = FreeCashFlow, NetCashFlow = NetCashFlow;
              """

    elif data_type == 'GetFinancialRatios':
        sql = """
              INSERT INTO CMoneyFinancialRatios (CMKey, BookValuePerShare, CurrentRatio, DateRange, DebtRatio, EPS, EarningsBeforeTaxesGrowthRate, EarningsBeforeTaxesRatio, EquityMultiplier, FixedAssetsTurnoverRatio, GearingRatio, GrossProfitMargin, InterestCover, InventoryTurnoverRatio, LiquidityRatio, NetProfitGrowthRatio, NetProfitMargin, OperatingProfitGrowthRate, OperatingProfitMargin, PayablesTurnoverRatio, ROA, ROE, ReceivablesTurnoverRatio, RevenueGrowthRate, RevenuePerShare, ShareholdersEquity, TotalAssets, TotalAssetsTurnoverRatio)
              VALUES ('""" + CMKey + """', %(BookValuePerShare)s, %(CurrentRatio)s, %(DateRange)s, %(DebtRatio)s, %(EPS)s, %(EarningsBeforeTaxesGrowthRate)s, %(EarningsBeforeTaxesRatio)s, %(EquityMultiplier)s, %(FixedAssetsTurnoverRatio)s, %(GearingRatio)s, %(GrossProfitMargin)s, %(InterestCover)s, %(InventoryTurnoverRatio)s, %(LiquidityRatio)s, %(NetProfitGrowthRatio)s, %(NetProfitMargin)s, %(OperatingProfitGrowthRate)s, %(OperatingProfitMargin)s, %(PayablesTurnoverRatio)s, %(ROA)s, %(ROE)s, %(ReceivablesTurnoverRatio)s, %(RevenueGrowthRate)s, %(RevenuePerShare)s, %(ShareholdersEquity)s, %(TotalAssets)s, %(TotalAssetsTurnoverRatio)s)
              ON DUPLICATE KEY UPDATE BookValuePerShare = BookValuePerShare, CurrentRatio = CurrentRatio, DebtRatio = DebtRatio, EPS = EPS, EarningsBeforeTaxesGrowthRate = EarningsBeforeTaxesGrowthRate, EarningsBeforeTaxesRatio = EarningsBeforeTaxesRatio, EquityMultiplier = EquityMultiplier, FixedAssetsTurnoverRatio = FixedAssetsTurnoverRatio, GearingRatio = GearingRatio, GrossProfitMargin = GrossProfitMargin, InterestCover = InterestCover, InventoryTurnoverRatio = InventoryTurnoverRatio, LiquidityRatio = LiquidityRatio, NetProfitGrowthRatio = NetProfitGrowthRatio, NetProfitMargin = NetProfitMargin, OperatingProfitGrowthRate = OperatingProfitGrowthRate, OperatingProfitMargin = OperatingProfitMargin, PayablesTurnoverRatio = PayablesTurnoverRatio, ROA = ROA, ROE = ROE, ReceivablesTurnoverRatio = ReceivablesTurnoverRatio, RevenueGrowthRate = RevenueGrowthRate, RevenuePerShare = RevenuePerShare, ShareholdersEquity = ShareholdersEquity, TotalAssets = TotalAssets, TotalAssetsTurnoverRatio = TotalAssetsTurnoverRatio;
              """

    with mysql_engine.connect() as connection:
        _ = connection.execute(sql, data)


@tenacity.retry(stop=tenacity.stop_after_attempt(30))
async def get_url(i, cmkey, url, headers, crawl_type, session):
    proxy = random.choice(proxies)
    async with session.get(url, proxy=proxy, headers=headers) as response:
        r = await response.text()
        data = json.loads(r)
        if crawl_type == 'mainstockimg':
            data = data['KImage']
        print(i, len(data))
        if len(data) != 0:
            config.data += len(data)
            insert_to_db(data, cmkey, crawl_type)


async def run_asyncio(cmkeys_mapping, cmkeys, crawl_type):
    urls = []
    headers = []
    new_cmkeys = []
    for cmkey in cmkeys:
        referer = f"https://www.cmoney.tw/finance/{cmkeys_mapping[crawl_type][1]}.aspx?s={cmkey[0]}"
        if crawl_type == '籌碼K線2':
            url = f"https://www.cmoney.tw/finance/ashx/MainPage.ashx?action={cmkeys_mapping[crawl_type][0]}&stockId={cmkey[0]}&days=3&cmkey={cmkeys_mapping['籌碼K線'][-1]}"
        elif crawl_type == '籌碼K線':
            url = f"https://www.cmoney.tw/finance/ashx/MainPage.ashx?action={cmkeys_mapping[crawl_type][0]}&stockId={cmkey[0]}&days=3&cmkey={cmkeys_mapping[crawl_type][-1]}"
        else:
            url = f"https://www.cmoney.tw/finance/ashx/mainpage.ashx?action={cmkeys_mapping[crawl_type][0]}&stockId={cmkey[0]}&cmkey={cmkeys_mapping[crawl_type][-1]}"    
        headers.append({'referer': referer})
        urls.append(url)
        new_cmkeys.append(cmkey[0])

    timeout = ClientTimeout(total=60)
    client_session = ClientSession(connector=TCPConnector(verify_ssl=False), timeout=timeout)
    tasks = [asyncio.create_task(get_url(i, new_cmkeys[i], url, headers[i], cmkeys_mapping[crawl_type][0], client_session)) for i, url in enumerate(urls)]
    await asyncio.gather(*tasks)


def run(crawl_type):
    new_cmkeys_mapping = get_cmkey(cmkeys_mapping)
    cmkeys = get_cmoney_stock_id()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_asyncio(new_cmkeys_mapping, cmkeys, crawl_type))
    return config.data
