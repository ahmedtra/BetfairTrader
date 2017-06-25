from data.quote_loader import DataLoader

if __name__ == "__main__":
    data_loader = DataLoader("1.128124452", 8768347)
    result = data_loader.load_df_data()
    print(result)
    print("here")