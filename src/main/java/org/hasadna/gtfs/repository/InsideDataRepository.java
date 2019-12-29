package org.hasadna.gtfs.repository;

import org.hasadna.gtfs.entity.InsideData;
import org.hasadna.gtfs.entity.RawData;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.query.QueryByExampleExecutor;

import java.util.List;
import java.util.Map;

public interface InsideDataRepository
        extends PagingAndSortingRepository<InsideData, String>, QueryByExampleExecutor<InsideData> {

    InsideData findFirstByDateAndRouteId(String date, String routeId);
    InsideData findFirstByDate(String date);

    List<InsideData> findByDate(String date);
    List<InsideData> findByDateAndRouteId(String date, String routeId);
    List<InsideData> findByDateAndRouteIdAndTripId(String date, String routeId, String tripId);

//    @Query("SELECT e.siriRawData FROM RawData e where e.date=:date and e.routeId=:routeId order by e.tripId")
//    List<String> findByDateAndRouteIdOrdered(@Param("date") final String date, @Param("routeId") final String routeId);


    long countByDate(String date);
    long countByDateAndRouteId(String date, String routeId);




    @Modifying
    @Query("delete from RawData f where f.date=:date")
    int deleteQueryByDate(@Param("date") String date);
}
