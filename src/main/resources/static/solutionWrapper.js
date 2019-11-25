// this variable is in top-level of script, so it should be accessible in all scripts
// initialized in this script, when solutionWrapper.html finished loading...
var allTripsFromJs;

// this var is initialized in selectLine.js according to the grid line selected by the user
var selectedRouteId ;


var tripIdsOnMap = [];
const defaultDisplayOptionsInWrapper = {
    displaySiri: true,
    displayStops: true,
    displayShape: true,
    displaySiriTooltipAlways: false
};
var displayOptionsInWrapper ;//= defaultDisplayOptionsInWrapper;

// expects date as a String in format "2019-03-31"
function fetchAllTripsForDay(routeId, date) {
    const url = 'siri/day/' + routeId + "/" + date;
    $.ajax({
        url: url
    })
        .done(function( json ) {
            allTripsFromJs.gtfsTrips = JSON.parse(json);
            clog("request completed, received " +
                    allTripsFromJs.gtfsTrips.length);

            populateTripsGrid(allTripsFromJs, routeId, date);
            clog("completed populating grid");
        });
}

// expects date as a String in format "2019-03-31"
function fetchJustStops(routeId, date) {
    // temporary
    routeId = "";
    tripId = "";
    date = "";
    // @GetMapping("gtfs/stops/{routeId}/{tripId}/{date}")
    const url = 'gtfs/stops/' + routeId + "/" + tripId + "/" + date;
    $.ajax({
        url: url
    })
        .done(function( json ) {
            allTripsFromJs.gtfsTrips = JSON.parse(json);
            clog("request completed, received " +
                allTripsFromJs.gtfsTrips.length);

            populateTripsGrid(allTripsFromJs);
            clog("completed populating grid");
        });
}

function removeLeadingZero(str) {
    if ((str) && (str[0] == '0')) {
        str = str[1];
    }
    return str;
}

function onlyHour(fullOad, date) {
    let oad = fullOad;
    if (fullOad.includes(date) && fullOad.includes("T")) {
        oad = fullOad.split("T")[1];
        let temp = oad.split(":");
        if (temp.length == 3) {
            oad = removeLeadingZero(temp[0]) + ":" + temp[1];
        }
    }
    return oad;
}

// changes the document by adding more lines to the table in element "all_lines"
// the new rows come from allTrips - the argument to this function.
// Called when this page loads.
// And probably also when user switches to another route or date (TBD).
//
// argument allTrips is an object with format like the sample in allTrips.js
//
function populateTripsGrid(allTrips, routeId, date) {
    const tableElement = document.getElementById('all_lines');
    const trips = allTrips.gtfsTrips.reverse();
    for (let i = 0; i < trips.length; i++) {
        let background = "info";
        let trip = trips[i];
        let tripId = trip.siriTripId;
        let vid = trip.vehicleId;
        if (true == trip.dns) {
            vid = "DNS";
            background = "danger";
        }
        if (trip.suspicious) {
            background = "warning";
        }
        let oad = onlyHour(trip.originalAimedDeparture, date);  // display only hour (if this hour is in the same day
        // generate tr
        let tr = "<tr class=\"clickable-row " + background + "\"><td>" + oad + "</td><td>" + tripId + "</td><td>" + vid + "</td></tr>";
        tableElement.innerHTML = tr + tableElement.innerHTML;
    }
}

// argument: tripId : String - the SIRI value of tripId
function findGtfsTripObject(tripId) {
    clog("value of allTripsFromJs is:" + allTripsFromJs);
    const gtfsTrip = allTripsFromJs.gtfsTrips.find(gTrip => gTrip.siriTripId == tripId);
    clog("found gtfsTrip for tripId=" + tripId);
    return gtfsTrip;
}


function populateRouteHeader(selectedRouteId, selectedDate) {
    const routeData = JSON.parse( sessionStorage.getItem("selectedRoute"));
    const dayOfWeek = dayOfWeek1(selectedDate);
    $('#routeHeader')[0].innerHTML =
        '<p>' + dayOfWeek + ' ' + selectedDate +  //' </p>\n' +
        '   <b>Route ' + selectedRouteId +  ',</b> ' + agencyCodeToName[routeData.agencyCode] + ' line ' + routeData.shortName + '</p>' +
        '<p><b style="background:#ccc; color:red;">FROM : </b>' + routeData.from + ' ' +
        '<b style="background:#ccc; color:red;">TO : </b>' + routeData.to + '</p>';
    let fromTo = '</b> from <span dir="rtl">' + routeData.from + '</span> to <span dir="rtl"></span>';
}

function populateBusHeader(selectedRouteId, selectedDate) {
    const routeData = JSON.parse(sessionStorage.getItem("selectedRoute"));
    $('#busHeader')[0].innerHTML =
        '<h4><strong> ' + agencyCodeToName[routeData.agencyCode] + ', line ' + routeData.shortName + ', route ' + routeData.routeId + ' </strong></h4>';
    let fromTo = ' from <span dir="rtl">' + routeData.from + '</span> to <span dir="rtl">' + routeData.to + '</span>';

}


/*
Please consider that the JS part isn't production ready at all, I just code it to show the concept of merging filters and titles together !
*/
$(document).ready(function() {
    // init displayOptions
    displayOptionsInWrapper = defaultDisplayOptionsInWrapper;
    $('#chk_siri').change(function(ev) {
        //console.log("chk_siri changed");
        displayOptionsInWrapper.displaySiri=ev.target.checked;
        iframe.contentWindow.toggleSiri(currentTripId);
        //console.log(displayOptionsInWrapper);
    });
    $('#chk_stops').change(function(ev) {
        displayOptionsInWrapper.displayStops=ev.target.checked;
        iframe.contentWindow.toggleStops(currentTripId);
    });
    $('#chk_shape').change(function(ev) {
        displayOptionsInWrapper.displayShape=ev.target.checked;
        iframe.contentWindow.toggleShape(currentTripId);
    });
    $('#chk_siri_tooltip').change(function(ev) {
        displayOptionsInWrapper.displaySiriTooltipAlways=ev.target.checked;
        iframe.contentWindow.toggleVisibilityOfTooltip(displayOptionsInWrapper.displaySiriTooltipAlways);
    });

    $('#v1').on('click',function(ev) {
        console.log(ev);
        //iframe.contentWindow.displayJustShape();
        //tripIdsOnMap.forEach(tripId => iframe.contentWindow.removeTripFromMap(tripId));
        console.log("clicked button v1, re-init map (this will undisplay all routes/shapes/markers on it)");
        iframe.contentWindow.initMap();
    });
    $('#vv4').on('click',function(ev) {
        console.log("clicked button vv4, toggle shape");

        iframe.contentWindow.toggleShape(currentTripId);
    });
    $('#vv2').on('click',function(ev) {
        console.log("clicked button vv2, toggle stops");
        iframe.contentWindow.toggleStops(currentTripId);
    });
    $('#vv3').on('click',function(ev) {
        console.log("clicked button vv3, toggle bus markers");
        iframe.contentWindow.toggleSiri(currentTripId);
        //iframe.contentWindow.undisplayTrip(currentTripId, false, true);
    });
    $('#v2').on('click',function(ev) {
        console.log("clicked button v2, calculate distance between stops");
        console.log(ev);
        let x = allTripsFromJs;
        let index = 0;

        while ((index < allTripsFromJs.gtfsTrips.length) && !allTripsFromJs.gtfsTrips[index].stops) {
            index = index + 1;
        }
        let z = allTripsFromJs.gtfsTrips[index].stops;
        iframe.contentWindow.measureDistanceBetweenStops(z);
    });
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

    var previousTripId;
    var currentTripId;

    // what happens when user clicks on one of the rows in the rips display on the right pane:
    $('#myTable').on('click', '.clickable-row', function(event) {
        $(this).addClass('active').siblings().removeClass('active');
        let tripId = event.currentTarget.children[1].textContent;
        clog("solutionWrapper: clicked line with tripId=" + tripId);
        let i = iframe.id;
        let setView = false;
        if (!previousTripId) {
            setView = true;
        }
        clog("setView=" + setView);

        // not needed, as we call displayAll which calls displayRouteOnMap
        //iframe.contentWindow.displayJustShape();

        //iframe.contentWindow.displayStopsOnMap(stops);  // 2nd arg optional, if not passed should use default
        /* expects stops.features
         stop.geometry.coordinates

"tripObject": {
        "shape": {
            "coordinates":[]
        }
        "stops" : {
            "features": [
                {
                    "geometry" : {
                        "coordinates":[]
                    }
                    "properties":[]
                },
                {...}
            ]
          }
*/

        //////////////////////////////
        //  original code - uses findGtfsTripObject() which uses a data structure already built before
        /////////////////////
        if (previousTripId) {   // first remove previous, then add current
            iframe.contentWindow.removeTripFromMap(previousTripId);
            clog("done removing trip " + previousTripId);
        }
        iframe.contentWindow.askDisplayAll(findGtfsTripObject(tripId), setView);
        tripIdsOnMap.push(tripId);
        previousTripId = tripId;
        currentTripId = tripId;
    });

    //allTripsFromJs = allTrips; // allTrips arrives from including allTrips.js. In production will arrive from Python analysis of Siri Data
    // initialize part of allTrips here hard coded:
    // (temporary code)
    allTripsFromJs = {
        gtfsRoute: {
            routeId: "7716",
            makat: "xxx",
            agencyId: "nn",
            agencyName: "Hebrew Characters",
            direction: "n",
            alternative: "ccc"
        },

        timeRange: {
            date: "2019-03-25"
        }
    };
    // initialize the Trips part by calling the web App
    selectedRouteId = sessionStorage.getItem("selectedRouteId");
    clog("selectedRouteId=" + selectedRouteId);
    selectedDate = sessionStorage.getItem("selectedDate");
    clog("selectedDate=" + selectedDate);
    if ("" === sessionStorage.getItem("shapeOfSelectedRoute")) {
        fetchShapeOfSelectedRoute(selectedDate, selectedRouteId);
    }

    //fetchAllTripsForDay("10812", "2019-04-18");
    fetchAllTripsForDay(selectedRouteId, selectedDate);
    // fetch json of trips by accessing the Java Spring controller
    // fetch is asynchronous, so after a few Seconds, when it completes,
    // populateTripsGrid(allTripsFromJs) will be called.
    populateBusHeader(selectedRouteId, selectedDate);
    populateRouteHeader(selectedRouteId, selectedDate);

});