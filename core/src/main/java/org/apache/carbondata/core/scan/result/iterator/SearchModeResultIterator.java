package org.apache.carbondata.core.scan.result.iterator;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.processor.BlockScan;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SearchModeResultIterator extends AbstractDetailQueryResultIterator<Object> {

    private final Object lock = new Object();

    private FileFactory.FileType fileType;
    private List<Future<BlockScan>> taskSubmitList;
    private BlockScan curBlockScan;
    private int nextBlockScanIndex = 0;

    public SearchModeResultIterator(List<BlockExecutionInfo> infos, QueryModel queryModel,
                                           ExecutorService execService) {
        super(infos, queryModel, execService);
        this.fileType = FileFactory.getFileType(queryModel.getAbsoluteTableIdentifier().getTablePath());
        scanAll();
    }

    private void scanAll() {
        taskSubmitList = new ArrayList<>(blockExecutionInfos.size());
        for (final BlockExecutionInfo info: blockExecutionInfos) {
            taskSubmitList.add(execService.submit(new Callable<BlockScan>() {

                @Override
                public BlockScan call() throws Exception {
                    BlockScan blockScan = new BlockScan(info, FileFactory.getFileHolder(fileType), buildQueryStatiticsModel(recorder));
                    blockScan.scan();
                    return blockScan;
                }
            }));
        }
    }

    @Override
    public boolean hasNext() {
        try {
            while ((curBlockScan == null || !curBlockScan.hasNext()) && nextBlockScanIndex < taskSubmitList.size()) {
                curBlockScan = taskSubmitList.get(nextBlockScanIndex++).get();
            }
            return curBlockScan != null && curBlockScan.hasNext();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object next() {
        throw new UnsupportedOperationException("call processNextBatch instead");
    }

    @Override
    public void processNextBatch(CarbonColumnarBatch columnarBatch) {
        synchronized (lock) {
            if (curBlockScan.hasNext()) {
                curBlockScan.processNextBatch(columnarBatch);
            }
        }
    }

    private QueryStatisticsModel buildQueryStatiticsModel(QueryStatisticsRecorder recorder) {
        QueryStatisticsModel queryStatisticsModel = new QueryStatisticsModel();
        queryStatisticsModel.setRecorder(recorder);
        QueryStatistic queryStatisticTotalBlocklet = new QueryStatistic();
        queryStatisticsModel.getStatisticsTypeAndObjMap()
                .put(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM, queryStatisticTotalBlocklet);
        queryStatisticsModel.getRecorder().recordStatistics(queryStatisticTotalBlocklet);

        QueryStatistic queryStatisticValidScanBlocklet = new QueryStatistic();
        queryStatisticsModel.getStatisticsTypeAndObjMap()
                .put(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM, queryStatisticValidScanBlocklet);
        queryStatisticsModel.getRecorder().recordStatistics(queryStatisticValidScanBlocklet);

        QueryStatistic totalNumberOfPages = new QueryStatistic();
        queryStatisticsModel.getStatisticsTypeAndObjMap()
                .put(QueryStatisticsConstants.TOTAL_PAGE_SCANNED, totalNumberOfPages);
        queryStatisticsModel.getRecorder().recordStatistics(totalNumberOfPages);

        QueryStatistic validPages = new QueryStatistic();
        queryStatisticsModel.getStatisticsTypeAndObjMap()
                .put(QueryStatisticsConstants.VALID_PAGE_SCANNED, validPages);
        queryStatisticsModel.getRecorder().recordStatistics(validPages);

        QueryStatistic scannedPages = new QueryStatistic();
        queryStatisticsModel.getStatisticsTypeAndObjMap()
                .put(QueryStatisticsConstants.PAGE_SCANNED, scannedPages);
        queryStatisticsModel.getRecorder().recordStatistics(scannedPages);

        QueryStatistic scanTime = new QueryStatistic();
        queryStatisticsModel.getStatisticsTypeAndObjMap()
                .put(QueryStatisticsConstants.SCAN_BLOCKlET_TIME, scanTime);
        queryStatisticsModel.getRecorder().recordStatistics(scanTime);

        QueryStatistic readTime = new QueryStatistic();
        queryStatisticsModel.getStatisticsTypeAndObjMap()
                .put(QueryStatisticsConstants.READ_BLOCKlET_TIME, readTime);
        queryStatisticsModel.getRecorder().recordStatistics(readTime);
        return queryStatisticsModel;
    }
}
