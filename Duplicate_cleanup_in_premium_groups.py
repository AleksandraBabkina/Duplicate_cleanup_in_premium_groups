from sqlalchemy import create_engine, Column, String, Float, select, or_, and_
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd

# Connection setup - DO NOT MODIFY
username = 'username'
password = 'password'
dsn = 'dsn'

conection_string = f'oracle+oracledb://{username}:{password}@{dsn}'  # Opening sql connection

# Create the connection engine
engine = create_engine(conection_string)  # Engine
Base = declarative_base()

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Direct query for data
query1 = """
select distinct agentgroup 

from v_tablefuncvalue
left join (select distinct prodgroupid, name region
           from v_prodgroup
           where inspproductid = 86 and 
           rulegroupid = 663) region on region.prodgroupid=parref1
           
left join (select distinct prodgroupid, name agenttype
           from v_prodgroup
           where inspproductid = 86 and 
           rulegroupid = 903) agenttype on agenttype.prodgroupid=parref3
           
left join (select distinct prodgroupid, name agentgroup
           from v_prodgroup 
           where inspproductid = 86 and 
           rulegroupid = 383
          ) agentgroup on agentgroup.prodgroupid=parref4   

where 1=1
 and datebeg =  (select max(datebeg) from v_tablefuncvalue 
 where tablefuncid = tablefuncid and datebeg <= trunc(sysdate)
 ) 

and tablefuncid = 3002
"""
sp = pd.read_sql(query1, engine)
sp = sp['agentgroup'].tolist()
sp


import pandas as pd
import time
from IPython.display import display

df = pd.DataFrame()
pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

def process_group(column, df):
    filter_df = rdf

    # Data processing using process_data
    filter_df = process_data(filter_df)

    previous_df = pd.DataFrame()
    final_combined_df = filter_df
    last_output_df = None

    while not previous_df.equals(final_combined_df):
        previous_df = final_combined_df
        final_combined_df = combine_rows(final_combined_df)
        last_output_df = final_combined_df  # Save the last result

        # Break the loop if there is only one row left in DataFrame
        if len(final_combined_df) == 0 or len(final_combined_df) == 1:
            print(f"There are no outliers in group {column}")
            return

    # Check for outliers after the loop ends
    group_sizes = final_combined_df.groupby(['region', 'tipts', 'grupaagentov', 'tipagenta']).size()
    if (group_sizes > 1).any():
        print(f"There are outliers in group {column}: \n")
        display(final_combined_df[final_combined_df.groupby(['region', 'tipts', 'grupaagentov', 'tipagenta']).transform('size') > 1])
    else:
        print(f"There are no outliers in group {column}")

def analyze_column(column, engine):
    query = f"""
select  valuenumber Value, region.region Region, parnum1 BSot, parnum2 BSdo,
case when parref2=1 then 'Motorcycles, mopeds and light quad bikes'
     when parref2=4 then 'Buses'
     when parref2=3 then 'Trucks'
     when parref2=2 then 'Passenger cars'
     when parref2=7 then 'Tractors, self-propelled road construction and other machines, except vehicles without wheels'
     when parref2=6 then 'Trams'
     when parref2=5 then 'Trolleybuses' end VehicleType, parstr1 TerOt, parstr2 TerDo,
parnum3 KOot, parnum4 KOdo, parnum5 KMot,  parnum6 KMdo, parnum7 KTot, parnum8 KTdo,
parnum9 ProLot, parnum10 ProLdo, parnum11 MinLot, parnum12 MinLdo, parnum13 YuLot, parnum14 YuLdo,
parnum15 KASKOot, parnum16 KASKOdo, agenttype.agenttype AgentType, agentgroup.agentgroup AgentGroup,
parnum17 KBMot, parnum18 KBMdo, parnum19 EOSAGOut, parnum20 EOSAGDo,
parnum21 DSot, parnum22 DDo, parnum23 KSot, parnum24 KSDO, parnum25 KPot, parnum26 KPdo, parnum27 PresenceOtherSkot, parnum28 PresenceOtherSdo
     
from v_tablefuncvalue
left join (select distinct prodgroupid, name region
           from v_prodgroup
           where inspproductid = 86 and 
           rulegroupid = 663) region on region.prodgroupid=parref1
           
left join (select distinct prodgroupid, name agenttype
           from v_prodgroup
           where inspproductid = 86 and 
           rulegroupid = 903) agenttype on agenttype.prodgroupid=parref3
           
left join (select distinct prodgroupid, name agentgroup
           from v_prodgroup 
           where inspproductid = 86 and 
           rulegroupid = 383
          ) agentgroup on agentgroup.prodgroupid=parref4   

where 1=1
 and datebeg =  (select max(datebeg) from v_tablefuncvalue 
 where tablefuncid = tablefuncid and datebeg <= trunc(sysdate)
 ) 

and tablefuncid = 3002
and agentgroup.agentgroup  in ('{column}') -- if you want to download all groups, comment this line
"""
    df = pd.read_sql(query, engine)
    return df

def process_data(df):
    # Convert 'VALUE' column to string type
    df['value'] = df['value'].astype(str)

    try:
        # Group by repeated values in specific columns
        group_counts = df.groupby(['value', 'region', 'bsot', 'bsdo', 'tipts', 'grupaagentov']).size()
        # Find groups that repeat more than once
        reaped_group = group_counts[group_counts > 1].index
        # Check if reaped_group is empty
        if reaped_group.empty:
            print(f"There are no outliers in group {column}")
            return df
        else:
            # Show these groups
            filter_df = df[df.set_index(['value', 'region', 'bsot', 'bsdo', 'tipts', 'grupaagentov']).index.isin(reaped_group)]
            
            # Find values with repetitions
            result_df = pd.DataFrame()
            for name, group in filter_df.groupby(['value', 'region', 'bsot', 'bsdo', 'tipts', 'grupaagentov']):
                if len(group["tipagenta"].unique()) == 2 and set(group["tipagenta"].unique()) == {"fl", "ul"}:
                    combaind_row = group.iloc[0].copy()
                    combaind_row["tipagenta"] = "fl\\ul"
                    result_df = pd.concat([result_df, pd.DataFrame([combaind_row])], ignore_index=True)
                    
            # Group by needed columns
            group_counts = result_df.groupby(['region', 'tipts', 'grupaagentov']).size()
            reaped_group = group_counts[group_counts > 1].index
            filter_df = result_df[result_df.set_index(['region', 'tipts', 'grupaagentov']).index.isin(reaped_group)]
            
    except KeyError as e:
        pass

    return filter_df

# New function
def combine_rows(filter_df):
    combined_df = pd.DataFrame()
    for name, group in filter_df.groupby(['region', 'types', 'agentgroup', 'agenttype']):
        group = group.drop_duplicates()
        while len(group) > 1:
            combinated = False
            for i in range(len(group)):
                for j in range(i+1, len(group)):
                    row1, row2 = group.iloc[i], group.iloc[j]
                    combinated_row = row1.copy()
                    # Check if the values in the rows are the same for specific columns
                    if (row1['terot'] == row2['terot'] and row1['terdo'] == row2['terdo']) and (str(row1['value']) in str(row2['value']) or str(row2['value']) in str(row1['value'])):
                        combinated_row['value'] = max(row1['value'], row2['value'])
                        combinated_row['terot'] = min(row1['terot'], row2['terot'])
                        combinated_row['terdo'] = max(row1['terdo'], row2['terdo'])
                        group = group.drop([group.index[i], group.index[j]])
                        combinated = True
                        break
                    elif (row1['bsot'] == row2['bsot'] and row1['bsdo'] == row2['bsdo']) and (str(row1['value']) in str(row2['value']) or str(row2['value']) in str(row1['value'])):
                        combinated_row['value'] = max(row1['value'], row2['value'])
                        combinated_row['bsot'] = min(row1['bsot'], row2['bsot'])
                        combinated_row['bsdo'] = max(row1['bsdo'], row2['bsdo'])
                        group = group.drop([group.index[i], group.index[j]])
                        combinated = True
                        break
                    # If both values contain slashes, process them separately
                    elif '/' in str(row1['value']) and '/' in str(row2['value']):
                        parts1 = str(row1['value']).split('/')
                        parts2 = str(row2['value']).split('/')
                        min_part = min(float(parts1[0]), float(parts2[0]))
                        max_part = max(float(parts1[1]), float(parts2[1]))
                        combinated_row['value'] = f"{min_part}/{max_part}"
                    # If neither value contains a slash, process them as regular numbers
                    elif '/' not in str(row1['value']) and '/' not in str(row2['value']):
                        combinated_row['value'] = f"{min(float(row1['value']), float(row2['value']))}/{max(float(row1['value']), float(row2['value']))}"
                    else:
                        break

                    # Check if one value contains the other
                    if str(row1['value']) in str(row2['value']):
                        combinated_row['value'] = row2['value']
                    elif str(row2['value']) in str(row1['value']):
                        combinated_row['value'] = row1['value']

                    # List of column pairs for updating values
                    value_pairs = [ ('bsot', 'bsdo'), ('terot', 'terdo'), ('kot', 'kdo'), ('kmot', 'kmdo'), ('ktot', 'ktdo'), 
                                   ('prolot', 'proldo'), ('minvot', 'minvdo'), ('ylot', 'yld'), ('kaskoout', 'kaskodo'), ('kbmot', 'kbmdo'),
                                   ('eosagoo', 'eosagod'), ('dsot', 'dsdo'), ('ksot', 'ksdo'), ('kpot', 'kpdo'), ('presenceofotherinsot', 'presenceofotherinsdo')]

                    # Update the pairs of values
                    for col_ot, col_do in value_pairs:
                        if col_ot in row1 and col_do in row1:
                            combinated_row[col_ot] = min(row1[col_ot], row2[col_ot])
                            combinated_row[col_do] = max(row1[col_do], row2[col_do])
                        
                    combined_df = pd.concat([combined_df, pd.DataFrame([combinated_row])], ignore_index=True)
                    group = group.drop([group.index[i], group.index[j]])
                    combinated = True
                    break
                if combinated:
                    break
        
        combined_df = pd.concat([combined_df, group], ignore_index=True)
    
    combined_df = combined_df.drop_duplicates()
    return combined_df

# Loop over each column and analyze the data
for column in sp:
    rdf = analyze_column(column, engine)
    process_group(column, df)
    time.sleep(0.3)
