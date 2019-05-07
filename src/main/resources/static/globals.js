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
