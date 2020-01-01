package org.hasadna.gtfs.repository;

import org.hasadna.gtfs.entity.GtfsStopTime;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.QueryByExampleExecutor;

import java.util.List;

public interface GtfsStopTimesRepository extends PagingAndSortingRepository<GtfsStopTime, String>, QueryByExampleExecutor<GtfsStopTime> {

    List<GtfsStopTime> findByDateAndTripId(String date, String tripId);

}
