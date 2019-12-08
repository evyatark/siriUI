package org.hasadna.gtfs.repository;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.hasadna.gtfs.entity.RawData;

import java.util.List;

public interface RawDataRepository
        extends PagingAndSortingRepository<RawData, String>, QueryByExampleExecutor<RawData> {


    List<RawData> findByDate(String date);
    List<RawData> findByDateAndRouteId(String date, String routeId);
    List<RawData> findByDateAndRouteIdAndTripId(String date, String routeId, String tripId);

    long countByDate(String date);
    long countByDateAndRouteId(String date, String routeId);

    long deleteByDate(String date);

    @Modifying
    @Query("delete from RawData f where f.date=:date")
    long deleteQueryByDate(@Param("date") String date);
}
