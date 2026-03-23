import pandas as pd

df = pd.read_csv("data/emergency-routing.csv")
df.columns = df.columns.str.strip()

# surrogate primary key
df.insert(0, "incident_id", range(1, len(df) + 1))

incident = df[[
    "incident_id", "Timestamp", "Incident_Type", "Incident_Severity",
    "Emergency_Level", "Region_Type", "Road_Type",
    "Number_of_Injuries", "Distance_to_Incident", "Response_Time", "Label"
]]

environmental = df[["incident_id", "Weather_Condition", "Weather_Impact",
                     "Traffic_Congestion", "Air_Traffic"]].copy()
environmental.insert(0, "condition_id", range(1, len(df) + 1))

resource = df[["incident_id", "Drone_Availability", "Ambulance_Availability",
               "Battery_Life", "Drone_Speed", "Ambulance_Speed",
               "Payload_Weight", "Fuel_Level"]].copy()
resource.insert(0, "resource_id", range(1, len(df) + 1))

dispatch = df[["incident_id", "Dispatch_Coordinator",
               "Specialist_Availability", "Hospital_Capacity"]].copy()
dispatch.insert(0, "dispatch_id", range(1, len(df) + 1))

# save
incident.to_parquet("../parquet-data/incident.parquet", index=False)
environmental.to_parquet("../parquet-data/environmental_conditions.parquet", index=False)
resource.to_parquet("../parquet-data/resource.parquet", index=False)
dispatch.to_parquet("../parquet-data/dispatch.parquet", index=False)