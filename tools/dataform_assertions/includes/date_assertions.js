/*
    Custom dataform assertions for date objects
*/


//TEST TO-DO
function test_future_date(colName){
    var result_query = "PARSE_DATE('%Y/%m/%d'," + colName+") < CURRENT_DATE()"
    return result_query
}

//TEST TO-DO
function test_valid_years(colName){
    var result_query = "DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y/%m/%d'," + colName + "), YEAR) < 100"
    return result_query
}

//TEST TO-DO
function test_date_format(colName, date_format){
    try {
        if(date_format == "mm/dd/yyyy" | date_format == "dd/mm/yyyy"){
            var result_query = "REGEXP_CONTAINS(" + colName + ", r'^[0-9]{4}[/][0-9]{2}[/][0-9]{2}$')"
            return result_query
        } else if (date_format == "yyyymmdd"){
            var result_query = "REGEXP_CONTAINS(" + colName + ", r'^[0-9]{4}[0-9]{2}[0-9]{2}$')"
            return result_query
        }
    } catch (error) {
        console.log("Please enter a date format that is 'yyyymmdd', 'mm/dd/yyyy', or 'dd/mm/yyyy'")
        return "Please enter a date format that is 'yyyymmdd', 'mm/dd/yyyy', or 'dd/mm/yyyy'"
    }
}

module.exports = {
    test_future_date,
    test_valid_years,
    test_date_format
}