$(document).ready(function() {
    $('.deliverable-infos').hide();
    $('.dropdown-deliverable').on('click', function(e) {
        console.log("dropdown toggled!");
        e.preventDefault();
        e.stopPropagation();
        //get targeted element via data-for attribute
        var dataFor = $(this).data('for');
        var idFor = $(dataFor);
        idFor.slideToggle();
    }); 
});