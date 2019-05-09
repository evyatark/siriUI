function whenDateFormSubmitted(event) {
    console.log(event);
}

function reverse(str) {
    // Step 1. Use the split() method to return a new array
    let splitString = str.split(""); // var splitString = "hello".split("");
    // ["h", "e", "l", "l", "o"]

    // Step 2. Use the reverse() method to reverse the new created array
    let reverseArray = splitString.reverse(); // var reverseArray = ["h", "e", "l", "l", "o"].reverse();
    // ["o", "l", "l", "e", "h"]

    // Step 3. Use the join() method to join all elements of the array into a string
    let joinArray = reverseArray.join(""); // var joinArray = ["o", "l", "l", "e", "h"].join("");
    // "olleh"

    //Step 4. Return the reversed string
    return joinArray; // "olleh"

}

function reverseDate(str) {
    let spl = str.split("/");
    return spl[2] + "-" + spl[1] + "-" + spl[0] ;
}

$(document).ready(function () {

    moment.locale('en');
    var date = new Date();
    bugun = moment(date).format("DD/MM/YYYY");

    var date_input = $('input[name="date"]'); //our date input has the name "date"
    var container = $('.bootstrap-iso form').length > 0 ? $('.bootstrap-iso form').parent() : "body";
    var options = {
        container: container,
        todayHighlight: true,
        autoclose: true,
        format: 'dd/mm/yyyy',
        language: 'en',
    };
    date_input.val(bugun);
    date_input.datepicker(options).on('focus', function (date_input) {
        $("h3").html("focus event");
    });



    date_input.change(function () {
        var deger = $(this).val();
        selectedDate = deger;
        sessionStorage.setItem("selectedDate", selectedDate);
        console.log('###########');
        $("h3").html("<font color=green>" + deger + "</font>");
    });


    $('.input-group').find('.glyphicon-calendar').on('click', function () {


        if (!date_input.data('datepicker').picker.is(":visible")) {
            date_input.trigger('focus');
            $("h3").html("Ok");

        } else {
        }


    });

});

