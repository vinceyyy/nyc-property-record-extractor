# %%
import requests
import pandas as pd
import json

# date range
start = 2015
end = 2020

# basic query for master record
def query(api, args=None):
    with open("secret.json") as j:
        secret = json.load(j)["secret"]
    headers = {
        "X-App-Token": secret,
    }
    if args is not None:
        url = api + args + "&$limit=99999999"
    else:
        url = api + "&$limit=99999999"
    # except:
    # url = api
    r = requests.get(
        url,
        headers=headers,
    )
    return r.json()


# query for legal and parties record based on master record
def query_sup(api, master_df):
    df = pd.DataFrame()
    # first, query by years covered in master record
    start = (
        master_df.sort_values("document_date")
        .head(1)
        .reset_index(drop=True)
        .loc[0, "document_date"][:4]
    )
    end = (
        master_df.sort_values("document_date")
        .tail(1)
        .reset_index(drop=True)
        .loc[0, "document_date"][:4]
    )
    years = range(int(start), int(end) + 1)
    for year in years:
        df = df.append(
            pd.DataFrame(query(api, f"$where=starts_with(document_id, '{year}')")),
            ignore_index=True,
        )
    # second, look for records that have different first four digit document_id than year, and query them via looping
    sup_df = master_df.loc[
        ~master_df["document_id"].str[:4].isin(str(year) for year in years)
    ]
    id_list = sup_df["document_id"].to_list()
    for id in id_list:
        sup = pd.DataFrame(query(api, f"document_id='{id}'"))
        df = df.append(sup, ignore_index=True)
    return df.drop("record_type", axis=1)


# api resources
acris_master = "https://data.cityofnewyork.us/resource/bnx9-e6tj.json?"
acris_legal = "https://data.cityofnewyork.us/resource/8h5j-fqxa.json?"
acris_parties = "https://data.cityofnewyork.us/resource/636b-3b5g.json?"

# %%
# get master record
master_raw = pd.DataFrame(
    query(
        acris_master,
        f"$where=document_date between '{start}-01-01T00:00:00' and '{end}-12-31T23:59:59' AND doc_type in('DEED', 'RPTT%26RET')",
    )
)
master = master_raw[["document_id", "document_date", "document_amt"]]

# %%
# get legal and parties record according to the master record
legal_raw = query_sup(acris_legal, master)
legal = legal_raw[
    [
        "document_id",
        "borough",
        "block",
        "lot",
        "property_type",
        "street_number",
        "street_name",
        "unit",
    ]
]
parties_raw = query_sup(acris_parties, master)
parties = parties_raw[parties_raw["party_type"] == "2"][
    ["document_id", "name"]
]  # keep only buyers' name

# %%
# merge all records to the master
df = master.merge(legal, on="document_id", how="left").merge(
    parties, on="document_id", how="left"
)

# cleaning
df.document_amt = df.document_amt.str.split(".").str[0].astype(int)
df.name = df.name.str.upper().str.strip()
df.document_date = pd.to_datetime(df.document_date)
df = df.rename(columns={"property_type": "ACRIS_property_type"})

# Identify df buyers, i.e. condo, apartment, coop, house
condo = ["BS", "CA", "CK", "MC", "SC", "SM"]
apar = ["AP"]
sf = ["D1"]
mf = ["D2", "D3", "D4", "D5", "D6", "F1", "F4", "RG", "RP", "RV"]
coop = ["MP", "SA", "SP"]
types = {"CD": condo, "AP": apar, "SF": sf, "MF": mf, "CP": coop}
df["property_type"] = "O"
for key, value in types.items():
    df.loc[df["ACRIS_property_type"].isin(value), "property_type"] = key


# mark chinese buyers
chn_df = pd.read_csv("chn_names.csv")
chn_names = []
# extract all value and split
for column in chn_df.columns[1:]:
    chn_names_col = chn_df[column].str.split("/").to_list()
    chn_names += chn_names_col
# drop nan, flatten the list, and drop duplicates
chn_names = [name for name in chn_names if type(name) is not float]
chn_names = list(set([n for name in chn_names for n in name]))
# clean it up
chn_names = [name.strip() for name in chn_names]
# I don't think people leave space in surname, even for two char surname
chn_names = [name.replace(" ", "-") for name in chn_names]
# add a new column to mark chn buyers (1 = chn, 2 = non_chn)
df["nationality"] = 2
df.loc[df["name"].str.split(",").str[0].isin(chn_names), "nationality"] = 1
# expand to record level: if one of the record is chinese buyer, the out put is chinese buyer (one record means one unique document_id and BBL)
marker = df.sort_values(["document_id", "nationality"]).drop_duplicates(
    subset=["document_id", "borough", "block", "lot"]
)[["document_id", "borough", "block", "lot", "nationality"]]


# drop duplicates
# records with the same document_id and BBL are the same record
# keep the one with the largest document_amt
unique = (
    df.drop("nationality", axis=1)
    .sort_values(["document_id", "document_amt"], ascending=False)
    .drop_duplicates(subset=["document_id", "borough", "block", "lot"], keep="first")
)
# merge with marker
output = unique.merge(marker, on=["document_id", "borough", "block", "lot"], how="left")

# %%
output.to_csv("output.csv", index=False)