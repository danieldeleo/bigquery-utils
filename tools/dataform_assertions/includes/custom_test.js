/*
Numerical Data Assertions
*/

/*
Telephone number checks
*/
//TEST DONE
function test_telephone_number_digits(colName){
    var result_query = ""
    result_query = 'LENGTH(' + colName+ ') = 10'
    return result_query
}

//TEST DONE
function test_telephone_number_start_with_zero(colName){
    var result_query = ""
    result_query = 'SUBSTRING(' + colName + ',1,1) != \'0\''
    return result_query
}

//TODO: TEST
function test_repeated_phone_number(colName, repeat_number){
    var result_query = ""
    result_query = colName + ' LIKE REPEAT(\'[0-9]\',' + repeat_number + ')'
    return result_query
}
/*
    Date Data Assertions
*/
//Support date format DD/MM/YYYY, MM/DD/YYYY
function test_date_format(colName, date_format){
    var result_query
    var index_string = date_format.charAt(0)
    if(index_string == 'd'){
        result_query = ''
    }
}


module.exports={
    /*
        Exporting numerical data assertions
     */
    test_telephone_number_digits,
    test_telephone_number_start_with_zero,
    test_repeated_phone_number,

    /* 
        Exporting date data assertions
    */
   test_date_format
}