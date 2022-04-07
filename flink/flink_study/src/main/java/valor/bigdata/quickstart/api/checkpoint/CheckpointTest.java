package valor.bigdata.quickstart.api.checkpoint;

/**
 * checkpoint(一致性检查点): 指在系统中所有算子对同一份输出都处理完了后，保存此时各个算子中的状态并将保存的state发送到jobManager/FsSystem/RocksDB。
 *  如果发生故障进行重启时，会将应用恢复到检查点的状态再进行处理
 *
 *  checkpoint barrier: 类似于在数据之间插入标识符，表明后续算子遇到这个标识符时就进行各自算子的checkpoint。
 *
 *  一致性：
 *
 * @author gary
 * @date 2022/3/23 9:45
 */
public class CheckpointTest {
    public static void main(String[] args) {

    }
}
