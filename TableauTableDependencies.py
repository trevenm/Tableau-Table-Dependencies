from tableau_api_lib import TableauServerConnection
import os
import pandas as pd
import pyodbc

# Retrieve environment variables
token_name = os.environ.get('Tableau_Token_Name')
token_value = os.environ.get('Tableau_Token_Value')
tableau_server = os.environ.get('Tableau_Server')
edw_cxn_string = os.environ.get('EDW_Connection_String')
target_table = os.environ.get('Tableau_Dependencies_Table')


# Open connection to Tableau Server
tableau_server_config = {
        'tableau_prod': {
                'server': tableau_server,
                'api_version': '3.17',
                'personal_access_token_name': token_name,
                'personal_access_token_secret': token_value,
                'site_name': '',
                'site_url': ''
        }
}

conn = TableauServerConnection(tableau_server_config)
conn.sign_in()


# Retrieve GraphQL query results to JSON
graphql_query = """
{
  workbooks {
    projectName
    name
    owner {
      name
    }
    embeddedDatasources {
      name
      upstreamTables {
        fullName
        database {
          name
        }
      }
    }
  }
}
"""

response = conn.metadata_graphql_query(query=graphql_query)
conn.sign_out()

# Transform JSON to Dataframe to prep for insert
tables = pd.json_normalize(response.json()["data"]["workbooks"]
  , record_path=["embeddedDatasources","upstreamTables"]
  , meta=["projectName", "name", ["datasourceName", "name"]]
  , meta_prefix="meta_"
  , errors="ignore")
tables_df = pd.DataFrame(tables)
tables_df = tables_df.rename(columns={"fullName":"TableName", "database.name":"Database", "meta_projectName":"ProjectName", "meta_name":"WorkbookName", "meta_datasourceName.name":"DatasourceName"})

datasources = pd.json_normalize(response.json()["data"]["workbooks"]
  , record_path=["embeddedDatasources"]
  , meta=["projectName", "name"]
  , meta_prefix="meta_"
  , errors="ignore")
datasources_df = pd.DataFrame(datasources)
datasources_df = datasources_df.drop(["upstreamTables"], axis=1)
datasources_df = datasources_df.rename(columns={"name":"DatasourceName", "meta_projectName":"ProjectName", "meta_name":"WorkbookName"})

workbooks = pd.json_normalize(response.json()["data"]["workbooks"])  
workbooks_df = pd.DataFrame(workbooks)
workbooks_df = workbooks_df.drop(["embeddedDatasources"], axis=1)
workbooks_df = workbooks_df.rename(columns={"projectName":"ProjectName", "name":"WorkbookName", "owner.name":"WorkbookOwner"})

joined = workbooks_df.merge(datasources_df, on=["WorkbookName", "ProjectName"], how="left")
joined = joined.merge(tables_df, on=["ProjectName", "WorkbookName", "DatasourceName"], how="left")
joined = joined.fillna("")

# Connect to SQL server and insert Dataframe to target table
cxn=pyodbc.connect(edw_cxn_string)
c=cxn.cursor()

c.execute(f"TRUNCATE TABLE {target_table}")
for index,row in joined.iterrows():
	c.execute(f"INSERT INTO {target_table} (ProjectName,WorkbookName,WorkbookOwner,DatasourceName,dbName,TableWithSchema) values(?,?,?,?,?,?)"
            , row.ProjectName, row.WorkbookName, row.WorkbookOwner, row.DatasourceName, row.Database, row.TableName)

cxn.commit()
cxn.close()






