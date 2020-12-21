function test_telephone_number(colName){
    return 'LENGTH('+colName+') = 10';
}


module.exports={
    test_telephone_number
}