package org.hasadna.gtfs.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.hasadna.gtfs.entity.RawData;

import java.util.List;
import java.util.Map;

public interface RawDataRepository
        extends PagingAndSortingRepository<RawData, String>, QueryByExampleExecutor<RawData> {
/*
    RawData findFirstByDateAndRouteId(String date, String routeId);
    RawData findFirstByDate(String date);

    List<RawData> findByDate(String date);
    List<RawData> findByDateAndRouteId(String date, String routeId);
    List<RawData> findByDateAndRouteIdAndTripId(String date, String routeId, String tripId);

    @Query("SELECT e.siriRawData FROM RawData e where e.date=:date and e.routeId=:routeId order by e.tripId")
    List<String> findByDateAndRouteIdOrdered(@Param("date") final String date, @Param("routeId") final String routeId);


    long countByDate(String date);
    long countByDateAndRouteId(String date, String routeId);


//    @Query("select u.lastname from User u group by u.lastname")
//    Page<String> findByLastnameGrouped(Pageable pageable);

    @Query("SELECT e.siriRawData, e.tripId FROM RawData e where e.date=:date and e.routeId=:routeId GROUP BY e.tripId")
    public Map<String,Object> findByDateAndRouteIdGroupByTripId(@Param("date") final String date, @Param("routeId") final String routeId);

    @Query(value = "select e.siriRawData, e.tripId from raw_data e where date = :date and route_id= :routeId group by e.trip_id", nativeQuery = true)
    List <Object[]> getRawDataGroupByTripId(@Param("date") final String date, @Param("routeId") final String routeId);

    @Query("select new map(v.tripId, v.siriRawData) from RawData v where date = :date and route_id= :routeId group by v.tripId")
    public List<?> findRawDataGrouped(@Param("date") final String date, @Param("routeId") final String routeId);

    long deleteByDate(String date);

    @Modifying
    @Query("delete from RawData f where f.date=:date")
    int deleteQueryByDate(@Param("date") String date);

 */
}
