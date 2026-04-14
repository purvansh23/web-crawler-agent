import pandas as pd

data = [
    {
        "Company_ID": "c001",
        "Company_Name": "Demo Logistics",
        "City": "Atlanta",
        "State": "GA",
        "Zip": "30303",
        "Website": "https://example.com"
    },
    {
        "Company_ID": "c002",
        "Company_Name": "Freight Hub Inc",
        "City": "Dallas",
        "State": "TX",
        "Zip": "75001",
        "Website": "https://test.com"
    }
]

df = pd.DataFrame(data)
df.to_excel("test_data.xlsx", index=False)
print("Created test_data.xlsx")
