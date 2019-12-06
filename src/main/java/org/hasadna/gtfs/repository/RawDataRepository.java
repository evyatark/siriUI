package org.hasadna.gtfs.repository;

import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.hasadna.gtfs.entity.RawData;

import java.util.List;

public interface RawDataRepository
        extends PagingAndSortingRepository<RawData, String>, QueryByExampleExecutor<RawData> {


    List<RawData> findByDate(String date);
    List<RawData> findByDateAndRouteId(String date, String routeId);
    List<RawData> findByDateAndRouteIdAndTripId(String date, String routeId, String tripId);

    long countByDate(String date);
}
