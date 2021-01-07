/*
    Custom dataform assertions to phone numbers
*/

/*
Telephone number checks
*/
//TEST TO-DO
function test_telephone_number_digits(colName){
    var result_query = 'LENGTH(' + colName+ ') = 10'
    return result_query
}

//TEST TO-DO
function test_telephone_number_start_with_zero(colName){
    var result_query = 'SUBSTRING(' + colName + ',1,1) != \'0\''
    return result_query
}

//TEST TO-DO
function test_repeated_phone_number(colName){
    var result_query = "TRIM(" + colName +", \"0\")!= \"\" AND " +
    "TRIM(" + colName +", \"1\")!= \"\" AND " + 
    "TRIM(" + colName +", \"2\")!= \"\" AND " +
    "TRIM(" + colName +", \"3\")!= \"\" AND " +
    "TRIM(" + colName +", \"4\")!= \"\" AND " +
    "TRIM(" + colName +", \"5\")!= \"\" AND " +
    "TRIM(" + colName +", \"6\")!= \"\" AND " +
    "TRIM(" + colName +", \"7\")!= \"\" AND " +
    "TRIM(" + colName +", \"8\")!= \"\" AND " +
    "TRIM(" + colName +", \"9\")!= \"\""
    return result_query
}

//TEST TO-DO
function test_only_contain_digit(colName){
    var result_query = "REGEXP_CONTAINS("+colName+", r'[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]')"
    return result_query
}

module.exports = {
    test_telephone_number_digits,
    test_telephone_number_start_with_zero,
    test_repeated_phone_number,
    test_only_contain_digit
}