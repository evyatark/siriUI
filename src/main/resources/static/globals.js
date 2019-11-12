var mapAllRoutesDisplayed ;
var allTripsFromJs ; // initialized in solution.html ready function, to content of allTrips

// this var is initialized in selectLine.js according to the grid line selected by the user
var selectedRouteId ;
var selectedDate;



var log = true;
function clog(arg) {
    if (log) {
        console.log(arg);
    }
}

var weekdays = new Array(
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
);

function dayOfWeek1(strDate) {
    const sp = strDate.split("-");
    const date = new Date(sp[0], sp[1]-1, sp[2]);

    return weekdays[date.getDay()];
}

const agencyCodeToName = {
    "16": "Superbus",
    "3": "Egged",
    "5": "Dan",
    "18": "Kavim",
    "15": "Metropolin",
    "25": "Afikim",
    "4": "EggedTaabura",
    "31": "DanBadarom",
    "14": "Nativ Express",
    "32": "DanBeerSheva",
    "30": "DanBatzafon"
};

function whenReceivingShape( json ) {
    clog("whenReceivingShape: request for shape completed, json=" + json);
    if (json) {
        clog(json.substring(0, 400));
    }
    else {
        clog("WARNING: empty json received from call to gtfs/shape/{routeId}/{date}");
        sessionStorage.setItem("shapeOfSelectedRoute", "");
        return;
    }
    clog("parsing json");
    let shape = JSON.parse(json);
    clog("request completed, received shape with length " + shape.shape.length);
    // all lines above are not necessary - only for debug
    sessionStorage.setItem("shapeOfSelectedRoute", JSON.stringify(shape.shape));
    clog("completed storing shape");
}

function fetchShapeOfSelectedRoute(selectedDate, selectedRouteId) {
    clog("requesting shape for route " + selectedRouteId + " on " + selectedDate);

    //const url = 'gtfs/trips/' + date + '/' + routeId;
    const url = 'gtfs/shape/' + selectedRouteId + '/' + selectedDate ;
    $.ajax({
        url: url
    }).done(whenReceivingShape);

}
