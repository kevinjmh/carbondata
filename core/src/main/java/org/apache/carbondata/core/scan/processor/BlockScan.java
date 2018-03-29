package org.apache.carbondata.core.scan.processor;

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.scan.collector.ResultCollectorFactory;
import org.apache.carbondata.core.scan.collector.ScannedResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.scanner.BlockletScanner;
import org.apache.carbondata.core.scan.scanner.impl.BlockletFilterScanner;
import org.apache.carbondata.core.scan.scanner.impl.BlockletFullScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

import java.util.LinkedList;
import java.util.List;

public class BlockScan {
    private BlockExecutionInfo blockExecutionInfo;
    private FileReader fileReader;
    private BlockletScanner blockletScanner;
    private BlockletIterator blockletIterator;
    private ScannedResultCollector scannerResultAggregator;

    private List<BlockletScannedResult> scannedResults = new LinkedList<>();
    private int nextResultIndex = 0;
    private BlockletScannedResult curResult;

    public BlockScan(BlockExecutionInfo blockExecutionInfo, FileReader fileReader, QueryStatisticsModel queryStatisticsModel) {
        this.blockExecutionInfo = blockExecutionInfo;
        this.fileReader = fileReader;
        this.blockletIterator = new BlockletIterator(blockExecutionInfo.getFirstDataBlock(),
                blockExecutionInfo.getNumberOfBlockToScan());
        if (blockExecutionInfo.getFilterExecuterTree() != null) {
            blockletScanner = new BlockletFilterScanner(blockExecutionInfo, queryStatisticsModel);
        } else {
            blockletScanner = new BlockletFullScanner(blockExecutionInfo, queryStatisticsModel);
        }
        this.scannerResultAggregator =
                ResultCollectorFactory.getScannedResultCollector(blockExecutionInfo);
    }

    public void scan() throws Exception {
        BlockletScannedResult blockletScannedResult = null;
        while (blockletIterator.hasNext()) {
            DataRefNode dataBlock = blockletIterator.next();
            if (dataBlock.getColumnsMaxValue() == null || blockletScanner.isScanRequired(dataBlock)) {
                RawBlockletColumnChunks rawBlockletColumnChunks =  RawBlockletColumnChunks.newInstance(
                        blockExecutionInfo.getTotalNumberDimensionToRead(),
                        blockExecutionInfo.getTotalNumberOfMeasureToRead(), fileReader, dataBlock);
                blockletScanner.readBlocklet(rawBlockletColumnChunks);
                blockletScannedResult = blockletScanner.scanBlocklet(rawBlockletColumnChunks);
                if (blockletScannedResult != null && blockletScannedResult.hasNext()) {
                    scannedResults.add(blockletScannedResult);
                }
            }

        }
    }

    public boolean hasNext() {
        if (curResult != null && curResult.hasNext()) {
            return true;
        } else {
            if (null != curResult) {
                curResult.freeMemory();
            }
            if (nextResultIndex < scannedResults.size()) {
                curResult = scannedResults.get(nextResultIndex++);
                return true;
            }else {
                return false;
            }
        }
    }

    public void processNextBatch(CarbonColumnarBatch columnarBatch) {
        this.scannerResultAggregator.collectResultInColumnarBatch(curResult, columnarBatch);
    }

}
