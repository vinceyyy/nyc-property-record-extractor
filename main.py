# %%
import requests
import pandas as pd
import json

# date range
start = 2018
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
    # by years
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
    # non regular id
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
master_raw = pd.DataFrame(
    query(
        acris_master,
        f"$where=document_date between '{start}-01-01T00:00:00' and '{end}-12-31T23:59:59' AND doc_type in('DEED', 'RPTT%26RET')",
    )
)
master = master_raw[["document_id", "document_date", "document_amt"]]

# %%
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
parties = parties_raw[parties_raw["party_type"] == "2"][["document_id", "name"]]

# %%
df = master.merge(legal, on="document_id", how="left").merge(
    parties, on="document_id", how="left"
)
df = df.sort_values("document_id").reset_index(drop=True)
df.document_amt = df.document_amt.str.split(".").str[0].astype(int)
