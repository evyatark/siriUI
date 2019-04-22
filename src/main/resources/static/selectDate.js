
//NOT NEEDED ANY SVGPathSegMovetoRel
// moved to inside html

$(document).ready(function(){

moment.locale('en');
//var ahmet = moment("25/04/2012","DD/MM/YYYY").year();
var date = new Date();
bugun = moment(date).format("DD/MM/YYYY");

      var date_input=$('input[name="date"]'); //our date input has the name "date"
      var container=$('.bootstrap-iso form').length>0 ? $('.bootstrap-iso form').parent() : "body";
      var options={
        //startDate: '+1d',
        //endDate: '+0d',
        container: container,
        todayHighlight: true,
        autoclose: true,
        format: 'dd/mm/yyyy',
        language: 'en',
        //defaultDate: moment().subtract(15, 'days')
        //setStartDate : "<DATETIME STRING HERE>"
      };
      date_input.val(bugun);
      date_input.datepicker(options).on('focus', function(date_input){
     $("h3").html("focus event");      
      }); ;
      
      
 date_input.change(function () {
    var deger = $(this).val();
    $("h3").html("<font color=green>" + deger + "</font>");
  });      
      
 
      
$('.input-group').find('.glyphicon-calendar').on('click', function(){
//date_input.trigger('focus');
//date_input.datepicker('show');
 //$("h3").html("event : click");


if( !date_input.data('datepicker').picker.is(":visible"))
{
       date_input.trigger('focus');
    $("h3").html("Ok"); 
 
    //$('.input-group').find('.glyphicon-calendar').blur();
    //date_input.trigger('blur');
    //$("h3").html("görünür");    
} else {
}


});      
 
 
 });
 </script>