var selectedRouteId ;
//var selectedDate;

const LIMIT = 10500;
/*
Please consider that the JS part isn't production ready at all, I just code it to show the concept of merging filters and titles together !
*/

/*
const log = true;
function clog(arg) {
    if (log) {
        console.log(arg);
    }
}
*/



// builds the HTML table under the HTML element with id 'all_lines'
function populateLinesGrid(allLinesOfDay) {
    let limitation = Math.min(LIMIT, allLinesOfDay.length);
    clog("populating grid with bus lines ... (limitation=" + limitation + ")");
    const tableElement = document.getElementById('all_lines');
    let innerHTML = "";
    for (let i = 0; i < limitation; i++) {
        let lineDetails = allLinesOfDay[i];
        let shortName = lineDetails.shortName;
        let routeId = lineDetails.routeId;
        if (i % 1000 == 0) {clog(i);}
        let to = lineDetails.to;
        let from = lineDetails.from;
        let agency = agencyCodeToName[lineDetails.agencyCode] || lineDetails.agencyCode ;
        //if (agency == undefined) {agency = lineDetails.agencyCode;}
        // generate td
        let td = "<tr class=\"clickable-row\"><td>" + agency + "</td><td>" + shortName + "</td><td>" + routeId + "</td><td>" + from + "</td><td>" + to + "</td></tr>";
        innerHTML = td + innerHTML;
    }
    tableElement.innerHTML = innerHTML;
    //clog("completed populating");
}

// expects date as a String in format "2019-03-31"
function fetchAllLinesForDay(date) {
    // selectedDate = localStorage.getItem("selectedDate");
    // console.log('############### '+selectedDate);
    clog("sending request for " + date);

    const url = 'gtfs/lines/' + date;
    $.ajax({
        url: url
    })
        .done(function( json ) {
            clog("request completed, parsing json");
            clog(json.substring(0, 400));
            let allLinesOfDay = JSON.parse(json);
            clog("request completed, received " + allLinesOfDay.length);

            populateLinesGrid(allLinesOfDay);
            clog("completed populating grid");
            sessionStorage.setItem("allRoutesOfDay", json); // note! we store the Json, not the array!
        });
}

$(document).ready(function () {

    selectedDate = sessionStorage.getItem("selectedDate");
    console.log('############### '+selectedDate);

    $('.filterable .btn-filter').click(function () {
        var $panel = $(this).parents('.filterable'),
            $filters = $panel.find('.filters input'),
            $tbody = $panel.find('.table tbody');
        if ($filters.prop('disabled') == true) {
            $filters.prop('disabled', false);
            $filters.first().focus();
        } else {
            $filters.val('').prop('disabled', true);
            $tbody.find('.no-result').remove();
            $tbody.find('tr').show();
        }
    });

    $('.filterable .filters input').keyup(function (e) {
        /* Ignore tab key */
        var code = e.keyCode || e.which;
        if (code == '9') return;
        /* Useful DOM data and selectors */
        var $input = $(this),
            inputContent = $input.val().toLowerCase(),
            $panel = $input.parents('.filterable'),
            column = $panel.find('.filters th').index($input.parents('th')),
            $table = $panel.find('.table'),
            $rows = $table.find('tbody tr');
        /* Dirtiest filter function ever ;) */
        var $filteredRows = $rows.filter(function () {
            var value = $(this).find('td').eq(column).text().toLowerCase();
            return value.indexOf(inputContent) === -1;
        });
        /* Clean previous no-result if exist */
        $table.find('tbody .no-result').remove();
        /* Show all rows, hide filtered ones (never do that outside of a demo ! xD) */
        $rows.show();
        $filteredRows.hide();
        /* Prepend no-result row if all rows are filtered */
        if ($filteredRows.length === $rows.length) {
            $table.find('tbody').prepend($('<tr class="no-result text-center"><td colspan="' + $table.find('.filters th').length + '">No result found</td></tr>'));
        }
    });

    $('#myTable').on('click', '.clickable-row', function (event) {
        $(this).addClass('active').siblings().removeClass('active');
        clog(event.currentTarget);
        //let shortPublishedName = event.target.parentElement.cells[0].innerText; // line name, e.g 480
        let activeRow = $('.active')[0];
        let shortPublishedName = activeRow.cells[1].innerText;
        let routeId = activeRow.cells[2].innerText;
        clog("selected: routeId=" + routeId + ", line name=" + shortPublishedName);
        selectedRouteId = routeId;
        sessionStorage.setItem("selectedRouteId", routeId);
        const arr = JSON.parse( sessionStorage.getItem("allRoutesOfDay"));   // array of {routeId, shortName, to, from, agencyCode
        const selectedItems = arr.filter(item => routeId == item.routeId);
        if (selectedItems != undefined && selectedItems.length > 0) {
            sessionStorage.setItem("selectedRoute", JSON.stringify(selectedItems[0]));
        }
    });

    console.log('############### '+ selectedDate);
    fetchAllLinesForDay(selectedDate); //"2019-04-18");
});