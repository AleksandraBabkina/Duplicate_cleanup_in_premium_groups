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
select distinct aggr.agentgroup

from diasoft_test.v_TABLEFUNCVALUE CV
left join (select distinct PRODGROUPID, NAME REGION
           from diasoft_test.v_PRODGROUP
           where INSPRODUCTID = 86 and 
           RULEGROUPID = 663) reg on reg.PRODGROUPID=CV.PARREF1
           
left join (select distinct PRODGROUPID, NAME agenttype
           from diasoft_test.v_PRODGROUP
           where INSPRODUCTID = 86 and 
           RULEGROUPID = 903) tipag on tipag.PRODGROUPID=CV.PARREF3
           
left join (select distinct PRODGROUPID, NAME agentgroup
           from diasoft_test.v_PRODGROUP 
           where INSPRODUCTID = 86 and 
           RULEGROUPID = 383
          ) aggr on aggr.PRODGROUPID=CV.PARREF4   

where 1=1
 and cv.DATEBEG =  (select max(CV1.datebeg) from diasoft_test.v_TABLEFUNCVALUE CV1 
 where cv1.TABLEFUNCID = cv.TABLEFUNCID and cv1.datebeg <=  trunc(sysdate)
 ) 

and TABLEFUNCID = 3002
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
    group_sizes = final_combined_df.groupby(['РЕГИОН', 'ТИПТС', 'ГРУППААГЕНТОВ', 'ТИПАГЕНТА']).size()
    if (group_sizes > 1).any():
        print(f"There are outliers in group {column}: \n")
        display(final_combined_df[final_combined_df.groupby(['РЕГИОН', 'ТИПТС', 'ГРУППААГЕНТОВ', 'ТИПАГЕНТА']).transform('size') > 1])
    else:
        print(f"There are no outliers in group {column}")

def analyze_column(column, engine):
    query = f"""
select  VALUENUMBER Value, reg.REGION Region, CV.PARNUM1 BSot, CV.PARNUM2 BSdo,
case when CV.PARREF2=1 then 'Motorcycles, mopeds and light quad bikes'
     when CV.PARREF2=4 then 'Buses'
     when CV.PARREF2=3 then 'Trucks'
     when CV.PARREF2=2 then 'Passenger cars'
     when CV.PARREF2=7 then 'Tractors, self-propelled road construction and other machines, except vehicles without wheels'
     when CV.PARREF2=6 then 'Trams'
     when CV.PARREF2=5 then 'Trolleybuses' end VehicleType, CV.PARSTR1 TerOt, CV.PARSTR2 TerDo,
CV.PARNUM3 KOot, CV.PARNUM4 KOdo, CV.PARNUM5 KMot,  CV.PARNUM6 KMdo, CV.PARNUM7 KTot, CV.PARNUM8 KTdo,
CV.PARNUM9 ProLot, CV.PARNUM10 ProLdo, CV.PARNUM11 MinLot, CV.PARNUM12 MinLdo, CV.PARNUM13 YuLot, CV.PARNUM14 YuLdo,
CV.PARNUM15 KASKOot, CV.PARNUM16 KASKOdo, tipag.agenttype AgentType, aggr.agentgroup AgentGroup,
CV.PARNUM17 KBMot, CV.PARNUM18 KBMdo, CV.PARNUM19 EOSAGOut, CV.PARNUM20 EOSAGDo,
CV.PARNUM21 DSot, CV.PARNUM22 DDo, CV.PARNUM23 KSot, CV.PARNUM24 KSDO, CV.PARNUM25 KPot, CV.PARNUM26 KPdo, cv.PARNUM27 PresenceOtherSkot, cv.PARNUM28 PresenceOtherSdo
     
from diasoft_test.v_TABLEFUNCVALUE CV
left join (select distinct PRODGROUPID, NAME REGION
           from diasoft_test.v_PRODGROUP
           where INSPRODUCTID = 86 and 
           RULEGROUPID = 663) reg on reg.PRODGROUPID=CV.PARREF1
           
left join (select distinct PRODGROUPID, NAME agenttype
           from diasoft_test.v_PRODGROUP
           where INSPRODUCTID = 86 and 
           RULEGROUPID = 903) tipag on tipag.PRODGROUPID=CV.PARREF3
           
left join (select distinct PRODGROUPID, NAME agentgroup
           from diasoft_test.v_PRODGROUP 
           where INSPRODUCTID = 86 and 
           RULEGROUPID = 383
          ) aggr on aggr.PRODGROUPID=CV.PARREF4   

where 1=1
 and cv.DATEBEG =  (select max(CV1.datebeg) from diasoft_test.v_TABLEFUNCVALUE CV1 
 where cv1.TABLEFUNCID = cv.TABLEFUNCID and cv1.datebeg <=  trunc(sysdate)
 ) 

and TABLEFUNCID = 3002
and aggr.agentgroup  in ('{column}') -- if you want to download all groups, comment this line
"""
    df = pd.read_sql(query, engine)
    return df

def process_data(df):
    # Convert 'VALUE' column to string type
    df['ЗНАЧЕНИЕ'] = df['ЗНАЧЕНИЕ'].astype(str)

    try:
        # Group by repeated values in specific columns
        group_counts = df.groupby(['ЗНАЧЕНИЕ', 'РЕГИОН', 'БСОТ', 'БСДО', 'ТИПТС', 'ГРУППААГЕНТОВ']).size()
        # Find groups that repeat more than once
        reaped_group = group_counts[group_counts > 1].index
        # Check if reaped_group is empty
        if reaped_group.empty:
            print(f"There are no outliers in group {column}")
            return df
        else:
            # Show these groups
            filter_df = df[df.set_index(['ЗНАЧЕНИЕ', 'РЕГИОН', 'БСОТ', 'БСДО', 'ТИПТС', 'ГРУППААГЕНТОВ']).index.isin(reaped_group)]
            
            # Find values with repetitions
            result_df = pd.DataFrame()
            for name, group in filter_df.groupby(['ЗНАЧЕНИЕ', 'РЕГИОН', 'БСОТ', 'БСДО', 'ТИПТС', 'ГРУППААГЕНТОВ']):
                if len(group["ТИПАГЕНТА"].unique()) == 2 and set(group["ТИПАГЕНТА"].unique()) == {"ФЛ", "ЮЛ"}:
                    combaind_row = group.iloc[0].copy()
                    combaind_row["ТИПАГЕНТА"] = "ФЛ\ЮЛ"
                    result_df = pd.concat([result_df, pd.DataFrame([combaind_row])], ignore_index=True)
                    
            # Group by needed columns
            group_counts = result_df.groupby(['РЕГИОН', 'ТИПТС', 'ГРУППААГЕНТОВ']).size()
            reaped_group = group_counts[group_counts > 1].index
            filter_df = result_df[result_df.set_index(['РЕГИОН', 'ТИПТС', 'ГРУППААГЕНТОВ']).index.isin(reaped_group)]
            
    except KeyError as e:
        pass

    return filter_df

# New function
def combine_rows(filter_df):
    combined_df = pd.DataFrame()
    for name, group in filter_df.groupby(['REGION', 'TYPES', 'AGENTGROUP', 'AGENTTYPE']):
        group = group.drop_duplicates()
        while len(group) > 1:
            combinated = False
            for i in range(len(group)):
                for j in range(i+1, len(group)):
                    row1, row2 = group.iloc[i], group.iloc[j]
                    combinated_row = row1.copy()
                    # Check if the values in the rows are the same for specific columns
                    if (row1['TEROT'] == row2['TEROT'] and row1['TERDO'] == row2['TERDO']) and (str(row1['VALUE']) in str(row2['VALUE']) or str(row2['VALUE']) in str(row1['VALUE'])):
                        combinated_row['VALUE'] = max(row1['VALUE'], row2['VALUE'])
                        combinated_row['TEROT'] = min(row1['TEROT'], row2['TEROT'])
                        combinated_row['TERDO'] = max(row1['TERDO'], row2['TERDO'])
                        group = group.drop([group.index[i], group.index[j]])
                        combinated = True
                        break
                    elif (row1['BSOT'] == row2['BSOT'] and row1['BSDO'] == row2['BSDO']) and (str(row1['VALUE']) in str(row2['VALUE']) or str(row2['VALUE']) in str(row1['VALUE'])):
                        combinated_row['VALUE'] = max(row1['VALUE'], row2['VALUE'])
                        combinated_row['BSOT'] = min(row1['BSOT'], row2['BSOT'])
                        combinated_row['BSDO'] = max(row1['BSDO'], row2['BSDO'])
                        group = group.drop([group.index[i], group.index[j]])
                        combinated = True
                        break
                    # If both values contain slashes, process them separately
                    elif '/' in str(row1['VALUE']) and '/' in str(row2['VALUE']):
                        parts1 = str(row1['VALUE']).split('/')
                        parts2 = str(row2['VALUE']).split('/')
                        min_part = min(float(parts1[0]), float(parts2[0]))
                        max_part = max(float(parts1[1]), float(parts2[1]))
                        combinated_row['VALUE'] = f"{min_part}/{max_part}"
                    # If neither value contains a slash, process them as regular numbers
                    elif '/' not in str(row1['VALUE']) and '/' not in str(row2['VALUE']):
                        combinated_row['VALUE'] = f"{min(float(row1['VALUE']), float(row2['VALUE']))}/{max(float(row1['VALUE']), float(row2['VALUE']))}"
                    else:
                        break

                    # Check if one value contains the other
                    if str(row1['VALUE']) in str(row2['VALUE']):
                        combinated_row['VALUE'] = row2['VALUE']
                    elif str(row2['VALUE']) in str(row1['VALUE']):
                        combinated_row['VALUE'] = row1['VALUE']

                    # List of column pairs for updating values
                    value_pairs = [ ('BSOT', 'BSDO'), ('TEROT', 'TERDO'), ('KOT', 'KDO'), ('KMOT', 'KMDO'), ('KTOT', 'KTDO'), 
                                   ('PROLOT', 'PROLDO'), ('MINVOT', 'MINVDO'), ('YLOT', 'YLD'), ('KASKOOUT', 'KASKODO'), ('KBMOT', 'KBMDO'),
                                   ('EOSAGOO', 'EOSAGOD'), ('DSOT', 'DSDO'), ('KSOT', 'KSDO'), ('KPOT', 'KPDO'), ('PRESENCEOFOTHERINSOT', 'PRESENCEOFOTHERINSDO')]

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
            if not combinated:
                break
        
        combined_df = pd.concat([combined_df, group], ignore_index=True)
    
    combined_df = combined_df.drop_duplicates()
    return combined_df

# Loop over each column and analyze the data
for column in sp:
    rdf = analyze_column(column, engine)
    process_group(column, df)
    time.sleep(0.3)