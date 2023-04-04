import requests
import urllib3 # for disable SSL warnings
import pandas as pd
import matplotlib.pyplot as plt

# API URLs list
BANKS = {
  "BB_API_URL": "https://opendata.api.bb.com.br",
  "BRADESCO_API_URL": "https://api.bradesco.com/bradesco",
  "CAIXA_API_URL": "https://api.openbanking.caixa.gov.br",
  "ITAU_API_URL": "https://api.itau",
  "PAN_API_URL": "https://api-openbanking.bancopan.com.br",
  "SANTANDER_API_URL": "https://openbanking.api.santander.com.br",
  "SICOOB_API_URL": "https://openfinance.sicoob.com.br",
  "SICREDI_API_URL": "https://api.sicredi.com.br"
}


def get_personal_credit_cards_data():
  endpoint = "/open-banking/products-services/v1/personal-credit-cards"
  bank_df = pd.DataFrame()
  for bank, host_url in BANKS.items():
    url = host_url + endpoint
    response = requests.get(url, verify=False)
    response_json = response.json()

    if response.status_code != 200:
      print(f"Error: {bank}")
      continue

    bank_df = bank_df._append(response_json["data"]["brand"]["companies"], ignore_index=True)

    while True:

      if "next" not in response_json["links"] or response_json["links"]["next"] == "" or response_json["links"]["next"] is None:
        break
      else:
        query_string = response_json["links"]["next"].split("?")[1]
        response_json = requests.get(url=url + '?' + query_string, verify=False).json()
        bank_df = bank_df._append(response_json["data"]["brand"]["companies"], ignore_index=True)

  bank_df = bank_df.drop(columns=['urlComplementaryList', 'cnpjNumber'])
  bank_df = bank_df.explode('personalCreditCards')

  bank_df['credit_card_name'] = bank_df['personalCreditCards'].apply(lambda x: x['name'])
  bank_df['credit_card_network'] = bank_df['personalCreditCards'].apply(lambda x: x['identification']['creditCard']['network'])
  bank_df['credit_card_type'] = bank_df['personalCreditCards'].apply(lambda x: x['identification']['product']['type'])

  bank_df['credit_card_fees_services'] = bank_df['personalCreditCards'].apply(lambda x: x['fees']['services'])
  bank_df = bank_df.explode('credit_card_fees_services')


  bank_df['credit_card_interest'] = bank_df['personalCreditCards'].apply(lambda x: x['interest'])

  bank_df['credit_card_interest_rates'] = bank_df['credit_card_interest'].apply(lambda x: x['rates'])
  bank_df = bank_df.explode('credit_card_interest_rates')

  bank_df['credit_card_interest_instalment_rates'] = bank_df['credit_card_interest'].apply(lambda x: x['instalmentRates'])
  bank_df = bank_df.explode('credit_card_interest_instalment_rates')

  bank_df = bank_df.drop(columns=['personalCreditCards'])

  print(bank_df.info())
  print(bank_df.head())
  print(bank_df.tail())

  # print min minimun interest rate for each bank
  # print(bank_df.groupby('name')['credit_card_interest_rates'])

  # display credit cards in a graph format (name x minimun interest rate)
  # bank_df.plot(x='name', y='credit_card_interest_rates_minimum', kind='bar', figsize=(20, 10))
  # plt.show()

def main():
  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) # disable SSL warnings
  get_personal_credit_cards_data()


if __name__ == "__main__":
  main()
