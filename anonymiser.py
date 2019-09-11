import pandas as pd
import os, zipfile
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, escape_name

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
    "Orders":{
        "Row ID":{
            "Type":"Integer",
            "Number Type":"Discrete",
            "Skip":False
        },
        "Order ID":{
            "Type":"Integer",
            "Number Type":"Discrete",
            "Skip":False
        },
        "Order Date":{
            "Type":"Date",
            "Skip":False
        },
        "Order Priority":{
            "Type":"String",
            "String Type":"Array",  # Required for String
            "Array Values":["N/A","L","M","H"], # Required for Array (user input)
            "Skip":False
        },
        "Order Quantity":{
            "Type":"Integer",
            "Number Type":"Continuous",
            "Skip":False
        },
        "Sales":{
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },
        "Discount":{
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },
        "Ship Mode":{
            "Type":"String",
            "String Type":"Array",  # Required for String
            "Array Values":["Air","Land","Sea"], # Required for Array (user input)
            "Array Probability":[0.5,0.3,0.2],
            "Skip":False
        },
        "Profit":{
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },
        "Unit Price":{
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },
        "Shipping Cost":{
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },
        "Customer Name":{
            "Type":"String",
            "String Type":"profile.name",
            "Skip":False
        },
        "City":{
            "Type":"String",
            "String Type":"geo.city",
            "Skip":False
        },
        "Zip Code":{
            "Type":"String",
            "String Type":"geo.zip",
            "Skip":False
        },
        "State":{
            "Type":"String",
            "String Type":"geo.state",
            "Skip":False
        },
        "Region":{
            "Type":"String",
            "String Type":"Array",
            "Array Values":["North","East","South","West"], # Required for Array (user input)
            "Array Probability":[0.2,0.3,0.2,0.3],
            "Skip":False
        },
        "Customer Segment":{
            "Type":"String",
            "String Type":"Array",
            "Array Values":["Samll Business","Corporate","Home Office","Consumer"], # Required for Array (user input)
            "Array Probability":[0.7,0.2,0.15,0.05],
            "Skip":False
        },
        "Product Category":{
            "Type":"String",
            "String Type":"Array",
            "Array Values":["Office Supplies","Furniture","Technology"], # Required for Array (user input)
            "Array Probability":[0.3,0.3,0.4],
            "Skip":False
        },
        "Product Sub-Category":{
            "Type":"String",
            "String Type":"lorem.word",
            "Number":2,
            "Skip":False
        },
        "Product Name":{
            "Type":"String",
            "String Type":"lorem.word",
            "Number":3,
            "Skip":False
        },
        "Product Container":{
            "Type":"String",
            "String Type":"Array",
            "Array Values":["Box","Pack","Wrap"], # Required for Array (user input)
            "Array Probability":[0.3,0.3,0.4],
            "Skip":False
        },
        "Product Base Margin":{
            "Type":"Float",
            "Decimal Places":2,
            "Skip":False
        },
        "Ship Date":{
            "Type":"Date",
            "Skip":False
        }
    }
]

###################
#
#  Initialize
#
###################

# Read data from file
# TO DO: change to CSV or some other handshake
def an0n(twbx_file_path,dataset_configuration,tempfolder)
    hyper_path = export_twbxobj_to_hyper(twbx_file_path,tempfolder)
    hyperfile_to_df(hyper_path)
    
    for extract in dataset_configuration:
        df = pd.read_excel(input_file)
        for column_name, column in extract:
            if not column["Skip"]:
                if column["Type"] == "Integer":
                    if column["Number Type"] = "Discrete":
                        df[column_name] = categorical(df[column_name])
                    elif column["Number Type"] = "Continuous":
                        df[column_name] = number_continuous(df[column_name],decimals=0)
                    else:
                        print("ERROR: Unhandled Number Type in column '%s'".format(column_name))
                elif column["Type"] == "Float":
                    df[column_name] = number_continuous(df[column_name],decimals=column["Decimal Places"])
                elif column["Type"] == "Date" or column["Type"] == "Time" or column["Type"] == "Datetime":
                    df[column_name] = datetime(df[column_name],datetime_part=column["Type"])
                elif column["Type"] == "String":
                    if column["String Type"].startswith("lorem."):
                        df[column_name] = lorem(type=column["String Type"][6:],number=column["Number"])
                    elif column["String Type"].startswith("geo."):
                        df[column_name] = geo(type=column["String Type"][4:])
                    elif column["String Type"] = "Array":
                        df[column_name] = string_array(values=column["Array Values"],probabilities=olumn["Array Probability"])
                        print("ERROR: Unhandled String Type in column '%s'".format(column_name))

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
