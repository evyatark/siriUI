// this var is initialized in selectLine.js according to the grid line selected by the user
var selectedRouteId ;
const LIMIT = 10500;

/*
const log = true;

function clog(arg) {
    if (log) {
        console.log(arg);
    }
}
*/


function populateTripsGrid(routeId, allTripsOfDay, date) {
    let limitation = Math.min(LIMIT, allTripsOfDay.length);
    clog("populating grid with bus trips ... (limitation=" + limitation + ")");
    const tableElement = document.getElementById('all_trips');
    let innerHTML = "";
    for (let i = 0; i < limitation; i++) {
        let lineDetails = allTripsOfDay[i];
        let shortName = "קו " + lineDetails.shortName;
        let tripId = lineDetails.gtfsTripId;
        if (i % 1000 == 0) {clog(i);}
        let oad = lineDetails.originalAimedDeparture;
        let status = lineDetails.status;
        let progress = lineDetails.progress;
        let agency = lineDetails.agencyName;
        let desc = lineDetails.description;
        let actualDeparture = lineDetails.actualDeparture;
        let suspicious = lineDetails.suspicious;
        if (suspicious) {status = "Suspicious";} else {status = "OK";}
        let label = "lable-info";
        if (suspicious) {label = "label-warning";}
        let tr1 = ' <tr class="clickable warning dropdown-deliverable" data-for="#details_' + i + '">\n' +
            '                <td>' + shortName + '</td>\n' +
            '                <td>' + tripId + '</td>\n' +
            '                <td>'+ oad +'</td>\n' +
            '                <td><span class="label ' + label + '">' + status + '</span></td>\n' +
            '                <td>\n' +
            '                    <div class="progress">\n' +
            '                        <div class="progress-bar progress-bar-success" style="width: 100%;">\n' +
            '                            ' + progress + '\n' +
            '                        </div>\n' +
            '                    </div>\n' +
            '                </td>\n' +
            '            </tr>';
        let tr2 = '<tr style="padding:0">\n' +
            '                <td style="padding:0"></td>\n' +
            '                <td colspan=2 style="padding:0px;">\n' +
            '                    <div class="deliverable-infos" id="details_' + i + '">\n' +
            '                        <table class="table table-condensed table-user-content" id="hidden_table_' + i + '">\n' +
            '                            <tbody>\n' +
            '                                <tr>\n' +
            '                                    <td>Date:</td>\n' +
            '                                    <td class="right-col">' + date + '</td>\n' +
            '                                </tr>\n' +
            '                                <tr>\n' +
            '                                    <td>Agency:</td>\n' +
            '                                    <td class="right-col">' + agency + '</td>\n' +
            '                                </tr>\n' +
            '                                <tr>\n' +
            '                                    <td>Line:</td>\n' +
            '                                    <td class="right-col">' + shortName + '</td>\n' +
            '                                </tr>\n' +
            '                                <tr>\n' +
            '                                    <td>Description:</td>\n' +
            '                                    <td class="right-col">' + desc + '</td>\n' +
            '                                </tr>\n' +
            '                                <tr>\n' +
            '                                    <td>TripId:</td>\n' +
            '                                    <td class="right-col">' + tripId + '</td>\n' +
            '                                </tr>\n' +
            '                                <tr>\n' +
            '                                    <td>Route ID:</td>\n' +
            '                                    <td class="right-col">' + routeId + '</td>\n' +
            '                                </tr>\n' +
            '\n' +
            '                                <tr>\n' +
            '                                    <td>Original Aimed Departue:</td>\n' +
            '                                    <td class="right-col">' + oad + '</td>\n' +
            '                                </tr>\n' +
            '                                <tr>\n' +
            '                                    <td>Actual Departure:</td>\n' +
            '                                    <td class="right-col">' + actualDeparture + '</td>\n' +
            '                                </tr>\n' +
            '                                <tr>\n' +
            '                                    <td>\n' +
            '                                        <a href="solutionWrapper.html" class="btn btn-primary" role="button">Investigate this trip on Map</a>\n' +
            '                                    </td>\n' +
            '\n' +
            '                                </tr>\n' +
            '                            </tbody>\n' +
            '                        </table>\n' +
            '                    </div>\n' +
            '                </td>\n' +
            '                <td style="padding:0"></td>\n' +
            '                <td style="padding:0"></td>\n' +
            '            </tr>' ;
        innerHTML = innerHTML + tr1 + tr2 ;
    }
    tableElement.innerHTML = innerHTML;
    prepareHide();
    //clog("completed populating");
    retrieveReport(date, routeId);
}

function prepareHide() {
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

}

function retrieveReport(date, routeId) {
    clog("retrieveReport");
}

function whenReceivingTrips( json ) {
    clog("request completed, parsing json");
    clog(json.substring(0, 400));
    let allTripsOfDay = JSON.parse(json);
    clog("request completed, received " + allTripsOfDay.length);

    populateTripsGrid(selectedRouteId, allTripsOfDay, selectedDate);
    clog("completed populating grid");
    $('#while_waiting').hide();
}

function whenReceivingShape( json ) {
    clog("request for shape completed, parsing json");
    clog(json.substring(0, 400));
    let shape = JSON.parse(json);
    clog("request completed, received shape with length " + shape.length);
    // all lines above are not necessary - only for debug
    sessionStorage.setItem("shapeOfSelectedRoute", json);
    clog("completed storing shape");
}

function fetchAllTripsForDayAndLine(date, routeId) {
    clog("sending request for " + date + " and " + routeId);

    //const url = 'gtfs/trips/' + date + '/' + routeId;
    const url = 'siri/day/' + routeId + "/" + date;
    $.ajax({
        url: url
    }).done(whenReceivingTrips);
}

function fetchShapeOfSelectedRoute(selectedDate, selectedRouteId) {
    clog("requesting shape for route " + selectedRouteId + " on " + selectedDate);

    //const url = 'gtfs/trips/' + date + '/' + routeId;
    const url = 'gtfs/shape/' + selectedRouteId + '/' + selectedDate ;
    $.ajax({
        url: url
    }).done(whenReceivingShape);

}

$(document).ready(function() {

    prepareHide();
    // initialize the Trips part by calling the web App
    selectedRouteId = sessionStorage.getItem("selectedRouteId");
    clog("selectedRouteId=" + selectedRouteId);

    selectedDate = sessionStorage.getItem("selectedDate");
    clog('selectedDate='+selectedDate);
    fetchAllTripsForDayAndLine(selectedDate, selectedRouteId); //"2019-04-18");
    fetchShapeOfSelectedRoute(selectedDate, selectedRouteId); // store in "shapeOfSelectedRoute"
});