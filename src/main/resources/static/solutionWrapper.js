// this variable is in top-level of script, so it should be accessible in all scripts
// initialized in this script, when solutionWrapper.html finished loading...
var allTripsFromJs;

// changes the document by adding more lines to the table in element "all_lines"
// the new rows come from allTrips - the argument to this function.
// Called when this page loads.
// And probably also when user switches to another route or date (TBD).
//
// argument allTrips is an object with format like the sample in allTrips.js
//
function populateTripsGrid(allTrips) {
    const tableElement = document.getElementById('all_lines');
    const trips = allTrips.gtfsTrips.reverse();
    for (let i = 0; i < trips.length; i++) {
        let trip = trips[i];
        let tripId = trip.siriTripId;
        let vid = trip.vehicleId;
        let oad = trip.originalAimedDeparture;
        // generate td
        let td = "<tr class=\"clickable-row\"><td>" + oad + "</td><td>" + tripId + "</td><td>" + vid + "</td></tr>";
        tableElement.innerHTML = td + tableElement.innerHTML;
    }
}

// argument: tripId : String - the SIRI value of tripId
function findGtfsTripObject(tripId) {
    console.log("value of allTripsFromJs is:" + allTripsFromJs);
    const gtfsTrip = allTripsFromJs.gtfsTrips.find(gTrip => gTrip.siriTripId == tripId);
    console.log("found gtfsTrip for tripId=" + tripId);
    return gtfsTrip;
}

/*
Please consider that the JS part isn't production ready at all, I just code it to show the concept of merging filters and titles together !
*/
$(document).ready(function(){
    $('.filterable .btn-filter').click(function(){
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

    $('.filterable .filters input').keyup(function(e){
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
        var $filteredRows = $rows.filter(function(){
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
            $table.find('tbody').prepend($('<tr class="no-result text-center"><td colspan="'+ $table.find('.filters th').length +'">No result found</td></tr>'));
        }
    });

    $('#myTable').on('click', '.clickable-row', function(event) {
        $(this).addClass('active').siblings().removeClass('active');
        let tripId = event.currentTarget.children[1].textContent;
        console.log("solutionWrapper: clicked line with tripId=" + tripId);
        let i = iframe.id;
        iframe.contentWindow.askDisplayAll(findGtfsTripObject(tripId));
        //askDisplayAll(findGtfsTripObject(tripId));
        //removeTripFromMap(previousTripId);
        //console.log("done removing trip " + previousTripId);
    });

    allTripsFromJs = allTrips; // allTrips arrives from including allTrips.js. In production will arrive from Python analysis of Siri Data
    console.log("solutionWrapper: allTripsFromJs initialized to allTrips (content of js file)")
    const sampleVehicleId = allTripsFromJs.gtfsTrips[0].vehicleId;
    console.log("solutionWrapper: vehicleId of first trip is " + sampleVehicleId);

    // populate the table with trips
    const numberOfTrips = allTripsFromJs.gtfsTrips.length;
    console.log("gtfs trips: " + numberOfTrips);
    populateTripsGrid(allTripsFromJs);


});