#######################################################################
# DEFINE CONSTANTS
#######################################################################
DEFAULT_INDEX_NAME = "opcv"
WEBSITE = "data.cityofnewyork.us"


#######################################################################
# SETTING UP DATA DICTIONARY FOR A NEW INDEX
#######################################################################

datetype = {"type":   "date","format": "MM/dd/YYYY"}
floattype = {"type":   "float"}
kwtype = {"type":   "keyword"}
texttype = {"type":   "text"}
summonstype = {"properties"  : {"url" : {"type":"text"}}}

estypes = {"date"    : datetype,
           "keyword" : kwtype  ,
           "text"    : texttype,
           "float"   : floattype,
           "s_image" : summonstype}

datatypedict = {"plate"                :"keyword",
                "state"                :"keyword",
                "license_type"         :"keyword",
                "summons_number"       :"keyword",
                "issue_date"           :"date",
                "violation_time"       :"keyword",
                "violation"            :"keyword",
                "judgment_entry_date"  :"date",
                "fine_amount"          :"float",
                "penalty_amount"       :"float",
                "interest_amount"      :"float",
                "reduction_amount"     :"float",
                "payment_amount"       :"float",
                "amount_due"           :"float",
                "precinct"             :"keyword",
                "county"               :"keyword",
                "issuing_agency"       :'keyword',
                "violation_status"     :"keyword",
                "summons_image"        :"s_image",
                "description"          :"text"
               }


index_data_dictionary = {k:estypes[v] for k,v in datatypedict.items()}
        
index_data_dictionary

#######################################################################
# FUNCTIONS FOR JSON CONVERSIONS
#######################################################################

def convert_value(a,typenew):
    if typenew == "float":
        return float(a)
    else:
        return a
        
def fixtypes(payload):
    return {c: convert_value(v,datatypedict[c]) for c,v in payload.items()}