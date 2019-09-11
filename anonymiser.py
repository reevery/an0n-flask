import pandas as pd
import os, zipfile
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, escape_name
from datetime import datetime

def export_twbxobj_to_hyper(fileobj,tempfolder):
    tmp_file_path = tempfolder + '/tmp.hyper'
    with open(tmp_file_path,'bw+') as tmp_file:
        with zipfile.ZipFile(fileobj,'r') as zfile:
            hyper_fileobj = zfile.read([z for z in zfile.namelist() if z.endswith('.hyper')][0])
            tmp_file.write(hyper_fileobj)
    return tmp_file_path

def hyperfile_to_df(hyper_path):
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,database=hyper_path) as connection:
            tablename = connection.catalog.get_table_names(schema="public")[0]
            table_definition = connection.catalog.get_table_definition(name=tablename)
            rows_in_table = connection.execute_result_list(query=f"SELECT * FROM {escape_name(tablename)}")
            df = pd.DataFrame(columns=[c.name for c in table_definition.columns],data=rows_in_table)
    return df



###################
#
#  Configuration
#
###################

dataset_configuration = [
    {
        'caption': "Orders",
        'fields': [
            {
                'name': "Row ID",
                "Type":"Integer",
                "Number Type":"Discrete",
                "Skip":False
            }, {
                'name':"Order ID",
            "Type":"Integer",
            "Number Type":"Discrete",
            "Skip":False
        },{
                'name':"Order Date",
            "Type":"Date",
            "Skip":False
        },{
                'name':"Order Priority",
            "Type":"String",
            "String Type":"Array",  # Required for String
            "Array Values":["N/A","L","M","H"], # Required for Array (user input)
            "Skip":False
        },{
                'name':"Order Quantity",
            "Type":"Integer",
            "Number Type":"Continuous",
            "Skip":False
        },{
                'name':"Sales",
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },{
                'name':"Discount",
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },{
                'name':"Ship Mode",
            "Type":"String",
            "String Type":"Array",  # Required for String
            "Array Values":["Air","Land","Sea"], # Required for Array (user input)
            "Array Probability":[0.5,0.3,0.2],
            "Skip":False
        },{
                'name':"Profit",
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },{
                'name':"Unit Price",
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },{
                'name':"Shipping Cost",
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },{
                'name':"Customer Name",
            "Type":"String",
            "String Type":"profile.name",
            "Skip":False
        },{
                'name':"City",
            "Type":"String",
            "String Type":"geo.city",
            "Skip":False
        },{
                'name':"Zip Code",
            "Type":"String",
            "String Type":"geo.zip",
            "Skip":False
        },{
                'name':"State",
            "Type":"String",
            "String Type":"geo.state",
            "Skip":False
        },{
                'name':"Region",
            "Type":"String",
            "String Type":"Array",
            "Array Values":["North","East","South","West"], # Required for Array (user input)
            "Array Probability":[0.2,0.3,0.2,0.3],
            "Skip":False
        },{
                'name':"Customer Segment",
            "Type":"String",
            "String Type":"Array",
            "Array Values":["Samll Business","Corporate","Home Office","Consumer"], # Required for Array (user input)
            "Array Probability":[0.7,0.2,0.15,0.05],
            "Skip":False
        },{
                'name':"Product Category",
            "Type":"String",
            "String Type":"Array",
            "Array Values":["Office Supplies","Furniture","Technology"], # Required for Array (user input)
            "Array Probability":[0.3,0.3,0.4],
            "Skip":False
        },{
                'name':"Product Sub-Category",
            "Type":"String",
            "String Type":"lorem.word",
            "Number":2,
            "Skip":False
        },{
                'name': "Product Name",
            "Type":"String",
            "String Type":"lorem.word",
            "Number":3,
            "Skip":False
        },{
                'name':"Product Container",
            "Type":"String",
            "String Type":"Array",
            "Array Values":["Box","Pack","Wrap"], # Required for Array (user input)
            "Array Probability":[0.3,0.3,0.4],
            "Skip":False
        },{
                'name':"Product Base Margin",
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },{
                'name':"Ship Date",
            "Type":"Date",
            "Skip":False
        }
        ]
    }
]

###################
#
#  Initialize
#
###################

# Read data from file
# TO DO: change to CSV or some other handshake
def an0n(twbx_file_path,dataset_configuration,tempfolder):
    hyper_path = export_twbxobj_to_hyper(twbx_file_path,tempfolder)
    df = hyperfile_to_df(hyper_path)
    
    for extract in dataset_configuration:
        for column in extract.fields :
            if not column["Skip"]:
                if column["Type"] == "Integer":
                    if column["Number Type"] == "Discrete":
                        df[column['name']] = categorical(df[column['name']])
                    elif column["Number Type"] == "Continuous":
                        df[column['name']] = number_continuous(df[column['name']],decimals=0)
                    else:
                        print("ERROR: Unhandled Number Type in column '%s'".format(column['name']))
                elif column["Type"] == "Float":
                    df[column['name']] = number_continuous(df[column['name']],decimals=column["Decimal Places"])
                elif column["Type"] == "Date" or column["Type"] == "Time" or column["Type"] == "Datetime":
                    df[column['name']] = datetime(df[column['name']],datetime_part=column["Type"])
                elif column["Type"] == "String":
                    if column["String Type"].startswith("lorem."):
                        df[column['name']] = lorem(type=column["String Type"][6:],number=column["Number"])
                    elif column["String Type"].startswith("geo."):
                        df[column['name']] = geo(type=column["String Type"][4:])
                    elif column["String Type"] == "Array":
                        df[column['name']] = string_array(values=column["Array Values"],probabilities=olumn["Array Probability"])
                        print("ERROR: Unhandled String Type in column '%s'".format(column['name']))

###################
#
#  Profiling
#
###################

###################
#
#  Data generation
#
###################

###################
#
#  Export to Extract SDK
#
###################
