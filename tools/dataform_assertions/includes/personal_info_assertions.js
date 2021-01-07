/*
    Custom dataform assertions for personal informations
*/

/* Email Assertions */
//TEST DONE
function test_email_validity(colName){
    var result_query = "REGEXP_CONTAINS(" + colName + ", r\"^[\\w.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$\")"
    return result_query
}

/* Marital Status */
//TEST DONE
function test_marital_status(colName){
    var marital_list = "[\"Married\", \"Single\", \"Divorced\", \"Widowed\"]"
    var result_query = colName + 
    " IN(SELECT marriage_status from UNNEST(" + marital_list+ ") AS marriage_status)"
    return result_query
}

/* Gender check */
//TEST DONE
function test_gender_status(colName){
    var gender_list = "[\"Female\",\"Male\",\"Transgender Female\",\"Transgender Male\",\"Gender Variant\",\"Prefer Not to Say\"]"
    var result_query = colName + 
    " IN(SELECT gender_status from UNNEST(" + gender_list + ") AS gender_status)"
    return result_query
}

/* Name check*/
//TEST DONE
function test_name_validity(colName){
    var result_query = "REGEXP_CONTAINS(" + colName + ", r\"^[a-zA-Z0-9]+$\")"
    return result_query
}

//TEST DONE
function test_last_name_unique(colName1, colName2){
    var result_query = colName1 + " != " + colName2
    return result_query
}

//TEST TODO
function test_same_character_more_than_two_times(colName){
    var regex = "(aaa)+|(bbb)+|(ccc)+|(ddd)+|(eee)+|(fff)+|(ggg)+|(hhh)+|(iii)+|(jjj)+|(kkk)+|(lll)+|(mmm)+|" + 
    "(nnn)+|(ooo)+|(ppp)+|(qqq)+|(rrr)+|(sss)+|(ttt)+|(uuu)+|(vvv)+|(www)+|(xxx)+|(yyy)+|(zzz)+"
    var query_result = "NOT REGEXP_CONTAINS(" + colName + ", r'"+ regex + "')"
    return query_result
}

module.exports = {
    test_email_validity,
    test_marital_status,
    test_gender_status,
    test_name_validity,
    test_last_name_unique,
    test_same_character_more_than_two_times
}