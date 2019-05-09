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