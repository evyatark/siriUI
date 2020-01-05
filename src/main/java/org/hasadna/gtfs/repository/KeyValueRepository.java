package org.hasadna.gtfs.repository;

import org.hasadna.gtfs.entity.GtfsStopTime;
import org.hasadna.gtfs.entity.KeyValue;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.QueryByExampleExecutor;

import java.util.List;

public interface KeyValueRepository extends PagingAndSortingRepository<KeyValue, String> {

    KeyValue findByKeyed(String keyed);

}
