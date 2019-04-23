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

