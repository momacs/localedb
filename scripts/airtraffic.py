# ALL IMPORTS
import os
import sys
import pandas as pd
import glob
import json
import requests
import time
import numpy as np
import urllib.request
from zipfile import ZipFile as zp


def timestamp(x, year):
    """Add timestamp YYYY-MM-DD"""
    DD = "01"
    MM = str(x)
    ts = f"{year}-{MM}-{DD}"
    return ts


def fips_it(x, fips_d):
    abbv = x[0]
    county = x[1]
    try:
        return fips_d[abbv][0][county]
    except:
        return "None"


def uncap_it(x):
    """Match capitalization format"""
    if "-" in str(x):
        temp = str(x).split("-")
        tt = ""
        for t in temp:
            tt = tt + "-" + t.capitalize()

        return tt[1:]

    else:
        temp = str(x).split()
        tt = ""
        for t in temp:
            tt = tt + " " + t.capitalize()

        return tt.lstrip()


def abrv_it(x):
    """Add state abbreviation"""
    abr = x.split(",")[1].lstrip()

    if len(abr) == 2:
        return abr
    else:
        return "None"


def admin1_origin(x, conv_d):
    """return admin1"""
    country = x[0]
    if country != "United States":

        city = x[1]

        if conv_d.get(country, "None") != "None":
            admin1 = conv_d[country][0].get(city)

            return admin1
    else:
        return x[2]


def admin2_it(x, county_d):
    """Add admin2 (County Name for US)"""
    if x[1] == "United States":

        return county_d.get(x[0], "None")
    else:
        return "None"


def download_lookups(wrkdir):
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/jataware/ASKE-weather/main/county_air_travel/abv_to_state.txt",
        f"{wrkdir}/abv_to_state.txt",
    )
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/jataware/ASKE-weather/main/county_air_travel/airportFD.txt",
        f"{wrkdir}/airportFD.txt",
    )
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/jataware/ASKE-weather/main/county_air_travel/county_to_fips.csv",
        f"{wrkdir}/county_to_fips.csv",
    )
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/jataware/ASKE-weather/main/county_air_travel/worldcities.csv",
        f"{wrkdir}/worldcities.csv",
    )


def cleanup(wrkdir, files):
    lookups = [
        f"{wrkdir}/abv_to_state.txt",
        f"{wrkdir}/airportFD.txt",
        f"{wrkdir}/county_to_fips.csv",
        f"{wrkdir}/worldcities.csv",
    ]
    for l in lookups:
        os.remove(l)

    for f in files:
        os.remove(f)

    others = glob.glob("**.csv")
    for o in others:
        os.remove(o)


def airtraffic(year, state, min_pax):
    wrkdir = os.getcwd()
    download_lookups(wrkdir)
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    }
    # f-string for 'year' selected above
    raw_data_dom = f"UserTableName=T_100_Domestic_Market__U.S._Carriers&DBShortName=Air_Carriers&RawDataTable=T_T100D_MARKET_US_CARRIER_ONLY&sqlstr=+SELECT+PASSENGERS%2CFREIGHT%2CMAIL%2CDISTANCE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CUNIQUE_CARRIER_NAME%2CUNIQUE_CARRIER_ENTITY%2CREGION%2CCARRIER%2CCARRIER_NAME%2CCARRIER_GROUP%2CCARRIER_GROUP_NEW%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CORIGIN_CITY_NAME%2CORIGIN_STATE_ABR%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_NM%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CDEST_CITY_NAME%2CDEST_STATE_ABR%2CDEST_STATE_FIPS%2CDEST_STATE_NM%2CDEST_WAC%2CYEAR%2CQUARTER%2CMONTH%2CDISTANCE_GROUP%2CCLASS+FROM++T_T100D_MARKET_US_CARRIER_ONLY+WHERE+YEAR%3D{year}&varlist=PASSENGERS%2CFREIGHT%2CMAIL%2CDISTANCE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CUNIQUE_CARRIER_NAME%2CUNIQUE_CARRIER_ENTITY%2CREGION%2CCARRIER%2CCARRIER_NAME%2CCARRIER_GROUP%2CCARRIER_GROUP_NEW%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CORIGIN_CITY_NAME%2CORIGIN_STATE_ABR%2CORIGIN_STATE_FIPS%2CORIGIN_STATE_NM%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CDEST_CITY_NAME%2CDEST_STATE_ABR%2CDEST_STATE_FIPS%2CDEST_STATE_NM%2CDEST_WAC%2CYEAR%2CQUARTER%2CMONTH%2CDISTANCE_GROUP%2CCLASS&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time=All%A0Months&timename=Month&GEOGRAPHY=All&XYEAR={year}&FREQUENCY=All&AllVars=All&VarName=PASSENGERS&VarDesc=Passengers&VarType=Num&VarName=FREIGHT&VarDesc=Freight&VarType=Num&VarName=MAIL&VarDesc=Mail&VarType=Num&VarName=DISTANCE&VarDesc=Distance&VarType=Num&VarName=UNIQUE_CARRIER&VarDesc=UniqueCarrier&VarType=Char&VarName=AIRLINE_ID&VarDesc=AirlineID&VarType=Num&VarName=UNIQUE_CARRIER_NAME&VarDesc=UniqueCarrierName&VarType=Char&VarName=UNIQUE_CARRIER_ENTITY&VarDesc=UniqCarrierEntity&VarType=Char&VarName=REGION&VarDesc=CarrierRegion&VarType=Char&VarName=CARRIER&VarDesc=Carrier&VarType=Char&VarName=CARRIER_NAME&VarDesc=CarrierName&VarType=Char&VarName=CARRIER_GROUP&VarDesc=CarrierGroup&VarType=Num&VarName=CARRIER_GROUP_NEW&VarDesc=CarrierGroupNew&VarType=Num&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarName=ORIGIN_CITY_NAME&VarDesc=OriginCityName&VarType=Char&VarName=ORIGIN_STATE_ABR&VarDesc=OriginState&VarType=Char&VarName=ORIGIN_STATE_FIPS&VarDesc=OriginStateFips&VarType=Char&VarName=ORIGIN_STATE_NM&VarDesc=OriginStateName&VarType=Char&VarName=ORIGIN_WAC&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarName=DEST_CITY_NAME&VarDesc=DestCityName&VarType=Char&VarName=DEST_STATE_ABR&VarDesc=DestState&VarType=Char&VarName=DEST_STATE_FIPS&VarDesc=DestStateFips&VarType=Char&VarName=DEST_STATE_NM&VarDesc=DestStateName&VarType=Char&VarName=DEST_WAC&VarDesc=DestWac&VarType=Num&VarName=YEAR&VarDesc=Year&VarType=Num&VarName=QUARTER&VarDesc=Quarter&VarType=Num&VarName=MONTH&VarDesc=Month&VarType=Num&VarName=DISTANCE_GROUP&VarDesc=DistanceGroup&VarType=Num&VarName=CLASS&VarDesc=Class&VarType=Char"
    # f-string for 'year' selected above
    raw_data_inter = f"UserTableName=T_100_International_Market__All_Carriers&DBShortName=Air_Carriers&RawDataTable=T_T100I_MARKET_ALL_CARRIER&sqlstr=+SELECT+PASSENGERS%2CFREIGHT%2CMAIL%2CDISTANCE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CUNIQUE_CARRIER_NAME%2CUNIQUE_CARRIER_ENTITY%2CREGION%2CCARRIER%2CCARRIER_NAME%2CCARRIER_GROUP%2CCARRIER_GROUP_NEW%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CORIGIN_CITY_NAME%2CORIGIN_COUNTRY%2CORIGIN_COUNTRY_NAME%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CDEST_CITY_NAME%2CDEST_COUNTRY%2CDEST_COUNTRY_NAME%2CDEST_WAC%2CYEAR%2CQUARTER%2CMONTH%2CDISTANCE_GROUP%2CCLASS+FROM++T_T100I_MARKET_ALL_CARRIER+WHERE+YEAR%3D{year}&varlist=PASSENGERS%2CFREIGHT%2CMAIL%2CDISTANCE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CUNIQUE_CARRIER_NAME%2CUNIQUE_CARRIER_ENTITY%2CREGION%2CCARRIER%2CCARRIER_NAME%2CCARRIER_GROUP%2CCARRIER_GROUP_NEW%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CORIGIN_CITY_NAME%2CORIGIN_COUNTRY%2CORIGIN_COUNTRY_NAME%2CORIGIN_WAC%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CDEST_CITY_NAME%2CDEST_COUNTRY%2CDEST_COUNTRY_NAME%2CDEST_WAC%2CYEAR%2CQUARTER%2CMONTH%2CDISTANCE_GROUP%2CCLASS&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time=All%A0Months&timename=Month&GEOGRAPHY=All&XYEAR={year}&FREQUENCY=All&AllVars=All&VarName=PASSENGERS&VarDesc=Passengers&VarType=Num&VarName=FREIGHT&VarDesc=Freight&VarType=Num&VarName=MAIL&VarDesc=Mail&VarType=Num&VarName=DISTANCE&VarDesc=Distance&VarType=Num&VarName=UNIQUE_CARRIER&VarDesc=UniqueCarrier&VarType=Char&VarName=AIRLINE_ID&VarDesc=AirlineID&VarType=Num&VarName=UNIQUE_CARRIER_NAME&VarDesc=UniqueCarrierName&VarType=Char&VarName=UNIQUE_CARRIER_ENTITY&VarDesc=UniqCarrierEntity&VarType=Char&VarName=REGION&VarDesc=CarrierRegion&VarType=Char&VarName=CARRIER&VarDesc=Carrier&VarType=Char&VarName=CARRIER_NAME&VarDesc=CarrierName&VarType=Char&VarName=CARRIER_GROUP&VarDesc=CarrierGroup&VarType=Num&VarName=CARRIER_GROUP_NEW&VarDesc=CarrierGroupNew&VarType=Num&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarName=ORIGIN_CITY_NAME&VarDesc=OriginCityName&VarType=Char&VarName=ORIGIN_COUNTRY&VarDesc=OriginCountry&VarType=Char&VarName=ORIGIN_COUNTRY_NAME&VarDesc=OriginCountryName&VarType=Char&VarName=ORIGIN_WAC&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarName=DEST_CITY_NAME&VarDesc=DestCityName&VarType=Char&VarName=DEST_COUNTRY&VarDesc=DestCountry&VarType=Char&VarName=DEST_COUNTRY_NAME&VarDesc=DestCountryName&VarType=Char&VarName=DEST_WAC&VarDesc=DestWac&VarType=Num&VarName=YEAR&VarDesc=Year&VarType=Num&VarName=QUARTER&VarDesc=Quarter&VarType=Num&VarName=MONTH&VarDesc=Month&VarType=Num&VarName=DISTANCE_GROUP&VarDesc=DistanceGroup&VarType=Num&VarName=CLASS&VarDesc=Class&VarType=Char"
    # DOMESTIC Get file name of zip file to be downloaded
    url_dom = "https://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=258&Has_Group=3&Is_Zipped=0"
    response = requests.post(url_dom, headers=headers, data=raw_data_dom)
    url_dom = response.url
    # International Get file name of zip file to be downloaded
    url_inter = "https://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=260&Has_Group=3&Is_Zipped=0"
    response = requests.post(url_inter, headers=headers, data=raw_data_inter)
    url_inter = response.url

    # Download and unzip the zip file

    # Files should look like these, but with different starting number string
    # url_dom = "https://transtats.bts.gov/ftproot/TranStatsData/982825318_T_T100D_MARKET_US_CARRIER_ONLY.zip"
    # url_inter = "https://transtats.bts.gov/ftproot/TranStatsData/982825318_T_T100I_MARKET_ALL_CARRIER.zip"

    urls = [url_dom, url_inter]

    files_to_unzip = []
    for url in urls:

        zip_file = url.split("/")[-1].split(".")[0]
        files_to_unzip.append(zip_file)

        downloaded = False
        tries = 0
        while not downloaded:
            try:
                remote = urllib.request.urlopen(url)
                data = remote.read()
                remote.close()
                downloaded = True
            except:
                print("WARNING: failed to download air traffic data. Retrying...") 
                tries += 1
                time.sleep(60)
            if tries == 2:
                sys.exit("ERROR: failed to download air traffic data from BTS.gov. Please try again later.")

        local = open(zip_file, "wb")
        local.write(data)
        local.close()

    # Unzip the downloaded aviation data
    for file_to_unzip in files_to_unzip:

        # specifying the zip file name
        file_name = file_to_unzip

        # opening the zip file in READ mode
        # with ZipFile(file_name, 'r') as zip:
        with zp(file_name, "r") as zip_:
            # printing all the contents of the zip file
            zip_.printdir()

            # extracting all the files
            print("Extracting all the files now...")
            zip_.extractall()
            print("Done!")

    # Domestic Only: Take off and land in US

    dom_fn = urls[0].split("/")[-1].split(".")[0] + ".csv"
    dom_dir = f"{wrkdir}/{dom_fn}"
    df_domestic = pd.read_csv(
        dom_dir,
        sep=",",
        converters={"PASSENGERS": lambda x: int(float(x))},
        engine="python",
    )

    # Add country name (headers from International data)
    df_domestic["ORIGIN_COUNTRY_NAME"] = "United States"
    df_domestic["ORIGIN_COUNTRY"] = "US"
    df_domestic["DEST_COUNTRY_NAME"] = "United States"
    df_domestic["DEST_COUNTRY"] = "US"

    # delete phantom column:
    df_domestic = df_domestic[
        df_domestic.columns.drop(list(df_domestic.filter(regex="Unnamed")))
    ]

    # add marker for domestic
    df_domestic["dataset"] = "domestic"

    dft = df_domestic.copy()

    # lower case all columns
    col_up = dft.columns

    col_low = [x.lower() for x in col_up]
    dft.columns = [x.lower() for x in dft.columns]
    dft = dft[col_low]

    # Delete zero pax
    dft = dft[dft["passengers"] != 0]

    # Delete rows with < min_pax
    dft = dft[dft["passengers"] >= min_pax]

    # Sort by month
    dft = dft.sort_values("month").reset_index(drop=True)

    # delete Saipan
    dft = dft[dft["origin_state_abr"] != "TT"]
    dft["timestamp"] = dft.month.apply(lambda x: timestamp(x, year))
    # Split to just city name
    dft["origin_city"] = dft.origin_city_name.apply(lambda x: x.split(",")[0])
    dft["dest_city"] = dft.dest_city_name.apply(lambda x: x.split(",")[0])
    # Rename columns for schema:
    dft = dft.rename(
        columns={
            "origin": "origin_airport_code",
            "dest": "dest_airport_code",
            "origin_country_name": "origin_admin0",
            "dest_country_name": "dest_admin0",
            "origin_country": "origin_iso2",
            "dest_country": "dest_iso2",
        }
    )
    df = dft.copy()

    # Add admin1 "Ohio"
    conv_state_fn = f"{wrkdir}/abv_to_state.txt"

    df_conv_state = pd.read_csv(conv_state_fn, sep="\t")
    state_conv_d = df_conv_state.set_index("Code").to_dict()
    state_conv_d = state_conv_d["Description"]

    df["origin_admin1"] = df.origin_state_abr.apply(
        lambda x: state_conv_d.get(x, "None")
    )
    df["dest_admin1"] = df.dest_state_abr.apply(lambda x: state_conv_d.get(x, "None"))

    # ADD COUNTY NAME

    # Read in Airport Facilities Directory data to get county name
    afd_fn = f"{wrkdir}/airportFD.txt"
    df_afd = pd.read_csv(afd_fn, sep="\t")

    # Build county NAME dictionary
    df_afd_county = pd.DataFrame(df_afd, columns=["LocationID", "County"])
    county_dict = df_afd_county.set_index("LocationID").to_dict()
    county_d = county_dict["County"]

    df["origin_admin2"] = df.origin_airport_code.apply(
        lambda x: county_d.get(x, np.NaN)
    )
    df["dest_admin2"] = df.dest_airport_code.apply(lambda x: county_d.get(x, np.NaN))

    # Replace #NAME? with NaN (for Puerto Rico)
    df.replace("#NAME?", "None", inplace=True)

    # ADD FIPS CODE

    # Add county FIPS from county NAME
    fips_fn = f"{wrkdir}/county_to_fips.csv"
    df_fips = pd.read_csv(
        fips_fn,
        sep=",",
        converters={"FIPS County Code": lambda x: str(x)},
        engine="python",
    )

    # Lookup dict
    fips_d = (
        df_fips.groupby("State")
        .apply(lambda x: [dict(zip(x["County Name"], x["FIPS County Code"]))])
        .to_dict()
    )

    df["munger_origin"] = df[["origin_state_abr", "origin_admin2"]].values.tolist()
    df["munger_dest"] = df[["dest_state_abr", "dest_admin2"]].values.tolist()

    df["origin_fips"] = df.munger_origin.apply(lambda x: fips_it(x, fips_d))
    df["dest_fips"] = df.munger_dest.apply(lambda x: fips_it(x, fips_d))

    # Format proper capitalization for County NAMEs
    df["origin_admin2"] = df["origin_admin2"].apply(lambda x: uncap_it(x))
    df["dest_admin2"] = df["dest_admin2"].apply(lambda x: uncap_it(x))

    # Get the columns we need
    df_domestic_final = df.copy()

    keepers = [
        "timestamp",
        "origin_airport_code",
        "origin_city",
        "origin_state_abr",
        "origin_admin2",
        "origin_fips",
        "origin_admin1",
        "origin_admin0",
        "origin_iso2",
        "dest_airport_code",
        "dest_city",
        "dest_state_abr",
        "dest_admin2",
        "dest_fips",
        "dest_admin1",
        "dest_admin0",
        "dest_iso2",
        "distance",
        "passengers",
        "month",
    ]
    df_domestic_final = df[keepers]

    # International: One (and only one) non-US location
    # for fn in os.listdir(wrkdir):
    #    if "T100I_MARKET_ALL_CARRIER.csv" in fn:
    #        file = fn

    inter_fn = urls[1].split("/")[-1].split(".")[0] + ".csv"
    inter_dir = f"{wrkdir}/{inter_fn}"
    df_international = pd.read_csv(
        inter_dir,
        sep=",",
        converters={"PASSENGERS": lambda x: int(float(x))},
        engine="python",
    )

    df = df_international.copy()

    # add marker for international flights
    df["dataset"] = "inter"

    # delete phantom column:
    df = df[df.columns.drop(list(df.filter(regex="Unnamed")))]

    # lower case all columns
    col_up = df.columns
    col_low = [x.lower() for x in col_up]
    df.columns = [x.lower() for x in df.columns]
    df = df[col_low]

    # Delete zero pax
    df = df[df["passengers"] != 0]

    # Delete rows with < min_pax
    df = df[df["passengers"] >= min_pax]

    # Sort by month
    df = df.sort_values("month").reset_index(drop=True)

    df["timestamp"] = df.month.apply(lambda x: timestamp(x, year))

    # Split to just city name
    df["origin_city"] = df.origin_city_name.apply(lambda x: x.split(",")[0])
    df["dest_city"] = df.dest_city_name.apply(lambda x: x.split(",")[0])

    # Rename columns for schema:
    df = df.rename(
        columns={
            "origin": "origin_airport_code",
            "dest": "dest_airport_code",
            "origin_country_name": "origin_admin0",
            "dest_country_name": "dest_admin0",
            "origin_country": "origin_iso2",
            "dest_country": "dest_iso2",
        }
    )

    df["origin_state_abr"] = df.origin_city_name.apply(lambda x: abrv_it(x))
    df["dest_state_abr"] = df.dest_city_name.apply(lambda x: abrv_it(x))

    # Add admin1 (state/province)
    conv_fn = f"{wrkdir}/worldcities.csv"
    df_conv = pd.read_csv(conv_fn, sep=",")
    df_conv["munger"] = df_conv[["iso2", "city_ascii", "admin_name"]].values.tolist()

    conv_d = (
        df_conv.groupby("country")
        .apply(lambda x: [dict(zip(x.city_ascii, x.admin_name))])
        .to_dict()
    )

    df["origin_admin1_temp"] = df.origin_state_abr.apply(
        lambda x: state_conv_d.get(x, "None")
    )
    df["dest_admin1_temp"] = df.dest_state_abr.apply(
        lambda x: state_conv_d.get(x, "None")
    )

    df["origin_munger"] = df[
        ["origin_admin0", "origin_city", "origin_admin1_temp"]
    ].values.tolist()
    df["dest_munger"] = df[
        ["dest_admin0", "dest_city", "dest_admin1_temp"]
    ].values.tolist()

    df["origin_admin1"] = df.origin_munger.apply(lambda x: admin1_origin(x, conv_d))
    df["dest_admin1"] = df.dest_munger.apply(lambda x: admin1_origin(x, conv_d))

    df["admin2org"] = df[["origin_airport_code", "origin_admin0"]].values.tolist()
    df["admin2dest"] = df[["dest_airport_code", "dest_admin0"]].values.tolist()

    df["origin_admin2"] = df.admin2org.apply(lambda x: admin2_it(x, county_d))
    df["dest_admin2"] = df.admin2dest.apply(lambda x: admin2_it(x, county_d))

    # Get fips for US airports

    df["munger_origin_fips"] = df[["origin_state_abr", "origin_admin2"]].values.tolist()
    df["munger_dest_fips"] = df[["dest_state_abr", "dest_admin2"]].values.tolist()

    df["origin_fips"] = df.munger_origin_fips.apply(lambda x: fips_it(x, fips_d))
    df["dest_fips"] = df.munger_dest_fips.apply(lambda x: fips_it(x, fips_d))

    # Format proper capitalization for County NAMEs
    df["origin_admin2"] = df["origin_admin2"].apply(lambda x: uncap_it(x))
    df["dest_admin2"] = df["dest_admin2"].apply(lambda x: uncap_it(x))

    # Get the columns we need
    df_inter_final = df.copy()

    keepers = [
        "timestamp",
        "origin_airport_code",
        "origin_city",
        "origin_fips",
        "origin_admin1",
        "origin_admin0",
        "dest_airport_code",
        "dest_city",
        "dest_fips",
        "dest_admin1",
        "dest_admin0",
        "distance",
        "passengers",
        "month",
    ]
    df_inter_final = df_inter_final[keepers]

    # Combine dataframes
    df_final = pd.concat([df_domestic_final, df_inter_final], ignore_index=True)

    df_final["distance"] = df_final.distance.apply(lambda x: int(x))

    cols = list(df_final.columns)
    cols.remove("passengers")
    grpr = ['origin_airport_code','dest_airport_code','timestamp']
    df_agg_ = df_final.groupby(grpr, as_index=False)[["passengers"]].sum()
    df_final_dedupe = df_final[cols].drop_duplicates()
    df_agg = pd.merge(df_agg_, df_final_dedupe,  how='left', 
                      left_on=['origin_airport_code','dest_airport_code', 'timestamp'],
                      right_on = ['origin_airport_code','dest_airport_code', 'timestamp'])        

    # Final cleanup
    df_agg.replace("#name?", np.nan, inplace=True)
    df_agg.replace("None", np.nan, inplace=True)

    df_agg = df_agg.sort_values("month").reset_index(drop=True)

    if state != '-':
        df_agg = df_agg[df_agg['dest_admin1']==state_conv_d[state]]

    df_agg = df_agg[keepers]
    del df_agg['month']

    df_agg = df_agg.rename(
        columns={
            "timestamp": "ts",
            "origin_airport_code": "origin_code",
            "dest_airport_code": "dest_code",
        }
    )

    cleanup(wrkdir, files_to_unzip)
    return df_agg

def gen_sql_update(origin_dest, merge_field,nullified=False):
    '''
    Takes in either origin or destination, and the field to merge on {fips, admin1, admin0}
    '''
    if nullified==True:
        addition = f"{origin_dest}_locale IS NULL AND "
    else:
        addition = ""
    sql = f"""
        UPDATE 
            mobility.airtraffic
        SET
            {origin_dest}_locale = locs.id
        FROM (
            SELECT * FROM
              (SELECT {origin_dest}_{merge_field} FROM mobility.airtraffic) AS air
              LEFT JOIN (SELECT id, {merge_field} FROM main.locale) AS locales
              ON air.{origin_dest}_{merge_field} = locales.{merge_field}) 
            AS locs
        WHERE {addition}
            airtraffic.{origin_dest}_{merge_field} = locs.{origin_dest}_{merge_field};
        """
    return sql