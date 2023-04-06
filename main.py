import requests
import urllib3  # for disable SSL warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

# API URLs list
BANKS = {
    "BANCO_DO_BRASIL": "https://opendata.api.bb.com.br",
    "BRADESCO": "https://api.bradesco.com/bradesco",
    "CAIXA": "https://api.openbanking.caixa.gov.br",
    "ITAU": "https://api.itau",
    "PAN": "https://api-openbanking.bancopan.com.br",
    "SANTANDER": "https://openbanking.api.santander.com.br",
    "SICOOB": "https://openfinance.sicoob.com.br",
    "SICREDI": "https://api.sicredi.com.br",
}


def main():
    urllib3.disable_warnings(
        urllib3.exceptions.InsecureRequestWarning
    )  # disable SSL warnings

    if not os.path.exists("data.parquet"):
        print("No parquet file found. Retrieving data from open finance API...")
        endpoint = "/open-banking/products-services/v1/personal-credit-cards"

        bank_df = pd.DataFrame(
            columns=[
                "bank_name",
                "credit_card_name",
                "credit_card_network",
                "credit_card_type",
                "credit_card_interest_rates",
                "credit_card_interest_instalment_rates",
            ]
        )

        for bank, host_url in BANKS.items():
            url = host_url + endpoint
            response = requests.get(url, verify=False)
            response_json = response.json()

            if response.status_code != 200:
                print(f"No data found for: {bank}")
                continue
            else:
                print(f"Getting data from: {bank}")

            for credit_card in response_json["data"]["brand"]["companies"]:
                for personal_credit_card in credit_card["personalCreditCards"]:
                    bank_df = bank_df._append(
                        {
                            "bank_name": response_json["data"]["brand"]["name"],
                            "credit_card_name": personal_credit_card["name"],
                            "credit_card_network": personal_credit_card[
                                "identification"
                            ]["creditCard"]["network"],
                            "credit_card_type": personal_credit_card["identification"][
                                "product"
                            ]["type"],
                            "credit_card_interest_rates": personal_credit_card[
                                "interest"
                            ]["rates"],
                            "credit_card_interest_instalment_rates": personal_credit_card[
                                "interest"
                            ][
                                "instalmentRates"
                            ],
                        },
                        ignore_index=True,
                    )

            while True:
                if (
                    "next" not in response_json["links"]
                    or response_json["links"]["next"] == ""
                    or response_json["links"]["next"] is None
                ):
                    break
                else:
                    query_string = response_json["links"]["next"].split("?")[1]
                    response_json = requests.get(
                        url=url + "?" + query_string, verify=False
                    ).json()

                    for credit_card in response_json["data"]["brand"]["companies"]:
                        for personal_credit_card in credit_card["personalCreditCards"]:
                            bank_df = bank_df._append(
                                {
                                    "bank_name": response_json["data"]["brand"]["name"],
                                    "credit_card_name": personal_credit_card["name"],
                                    "credit_card_network": personal_credit_card[
                                        "identification"
                                    ]["creditCard"]["network"],
                                    "credit_card_type": personal_credit_card[
                                        "identification"
                                    ]["product"]["type"],
                                    "credit_card_interest_rates": personal_credit_card[
                                        "interest"
                                    ]["rates"],
                                    "credit_card_interest_instalment_rates": personal_credit_card[
                                        "interest"
                                    ][
                                        "instalmentRates"
                                    ],
                                },
                                ignore_index=True,
                            )

        # min and max interest rates
        bank_df["min_interest_rate"] = bank_df["credit_card_interest_rates"].apply(
            lambda x: [rate["minimumRate"] for rate in x]
        )

        bank_df["min_instalment_interest_rate"] = bank_df[
            "credit_card_interest_instalment_rates"
        ].apply(lambda x: [rate["minimumRate"] for rate in x])

        bank_df["max_interest_rate"] = bank_df["credit_card_interest_rates"].apply(
            lambda x: [rate["maximumRate"] for rate in x]
        )

        bank_df["max_instalment_interest_rate"] = bank_df[
            "credit_card_interest_instalment_rates"
        ].apply(lambda x: [rate["maximumRate"] for rate in x])

        # np.nan values for empty lists
        bank_df["min_interest_rate"] = bank_df["min_interest_rate"].apply(
            lambda x: np.nan if len(x) == 0 else x
        )
        bank_df["min_instalment_interest_rate"] = bank_df[
            "min_instalment_interest_rate"
        ].apply(lambda x: np.nan if len(x) == 0 else x)

        bank_df["max_interest_rate"] = bank_df["max_interest_rate"].apply(
            lambda x: np.nan if len(x) == 0 else x
        )

        bank_df["max_instalment_interest_rate"] = bank_df[
            "max_instalment_interest_rate"
        ].apply(lambda x: np.nan if len(x) == 0 else x)

        # convert "list" values to "float"
        bank_df["min_interest_rate"] = bank_df["min_interest_rate"].apply(
            lambda x: x[0] if type(x) == list else x
        )
        bank_df["min_instalment_interest_rate"] = bank_df[
            "min_instalment_interest_rate"
        ].apply(lambda x: x[0] if type(x) == list else x)

        bank_df["max_interest_rate"] = bank_df["max_interest_rate"].apply(
            lambda x: x[0] if type(x) == list else x
        )
        bank_df["max_instalment_interest_rate"] = bank_df[
            "max_instalment_interest_rate"
        ].apply(lambda x: x[0] if type(x) == list else x)
    else:
        print("Parquet file found. Loading data from file...")
        bank_df = pd.read_parquet("data.parquet")

    # generate a dataframe copy without nan values in min_interest_rate column
    bank_df_no_nan_min_interest_rate = bank_df[bank_df["min_interest_rate"].notna()]
    min_interest_rate = bank_df_no_nan_min_interest_rate.groupby("bank_name")[
        "min_interest_rate"
    ].min()

    # generate a dataframe copy without nan values in min_instalment_interest_rate column
    bank_df_no_nan_min_instalment_interest_rate = bank_df[
        bank_df["min_instalment_interest_rate"].notna()
    ]
    min_instalment_interest_rate = bank_df_no_nan_min_instalment_interest_rate.groupby(
        "bank_name"
    )["min_instalment_interest_rate"].min()

    # generate a dataframe copy without nan values in max_interest_rate column
    bank_df_no_nan_max_interest_rate = bank_df[bank_df["max_interest_rate"].notna()]
    max_interest_rate = bank_df_no_nan_max_interest_rate.groupby("bank_name")[
        "max_interest_rate"
    ].max()

    # generate a dataframe copy without nan values in max_instalment_interest_rate column
    bank_df_no_nan_max_instalment_interest_rate = bank_df[
        bank_df["max_instalment_interest_rate"].notna()
    ]
    max_instalment_interest_rate = bank_df_no_nan_max_instalment_interest_rate.groupby(
        "bank_name"
    )["max_instalment_interest_rate"].max()

    # generate a dataframe with all rates (min, max, min_instalment, max_instalment) per bank and print it
    bank_df_rates = pd.concat(
        [
            min_interest_rate,
            max_interest_rate,
            min_instalment_interest_rate,
            max_instalment_interest_rate,
        ],
        axis=1,
    )
    print(bank_df_rates)

    bank_df.to_parquet("data.parquet")


if __name__ == "__main__":
    main()
