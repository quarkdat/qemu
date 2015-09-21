
#include <inttypes.h>

#include "qemu-common.h"
#include "qemu/error-report.h"
#include "block/block_int.h"

#include <shake.h>
#include <stdio.h>

typedef enum {
    SHAKE_AIO_READ = 0,
    SHAKE_AIO_WRITE,
    SHAKE_AIO_DISCARD,
    SHAKE_AIO_FLUSH
} ShakeAIOCmd;

typedef struct ShakeImage {
    char name[MAX_SHAKE_IMG_NAME_SIZE];
    img_t *img;
} ShakeImage;

typedef struct ShakeAIOCB {
    BlockAIOCB common;
    QEMUBH *bh;
    QEMUIOVector *qiov;
    struct ShakeImage *si;
    ShakeAIOCmd cmd;
    img_iocb_t iocb;
    char *buf;
    int64_t ret;
} ShakeAIOCB;


static QemuOptsList runtime_opts = {
    .name = "shake",
    .head = QTAILQ_HEAD_INITIALIZER(runtime_opts.head),
    .desc = {
        {
            .name = "filename",
            .type = QEMU_OPT_STRING,
            .help = "Specification of the shake image",
        },
        { /*end of list*/ }
    },
};

static int qemu_shake_parsename(const char *filename,
                                char *img_name,
                                int name_len,
                                Error **errp)
{
    /* error_report(" ===== qemu_shake_parsename"); // TODO */
    const char *start;

    if (!strstart(filename, "shake:", &start)) {
        error_setg(errp, "File name must start with 'shake:'");
        return -EINVAL;
    }

    if (strlen(start) > name_len) {
        error_setg(errp, "%s too long", start);
        return -EINVAL;
    }

    memcpy(img_name, start, strlen(start));
    img_name[strlen(start)] = '\0'; 
    return 0;
}

static void qemu_shake_cleanup(ShakeImage *si)
{
    /* error_report(" ===== qemu_shake_cleanup"); // TODO */
    if (si) {
        if (si->img) img_close(si->img);
    }
}

static int qemu_shake_open(BlockDriverState *bs, QDict *options, int flags,
                           Error **errp)
{
    /* error_report(" ===== qemu_shake_open "); // TODO */

    ShakeImage *si = bs->opaque;
    const char *filename;
    QemuOpts *opts;
    Error *local_err = NULL;

    //过滤options
    opts = qemu_opts_create(&runtime_opts, NULL, 0, &error_abort);
    qemu_opts_absorb_qdict(opts, options, &local_err);
    if (local_err) {
        error_propagate(errp, local_err);
        qemu_opts_del(opts);
        return -EINVAL;
    }

    // filename的格式是 shake:/{image_name}
    filename = qemu_opt_get(opts, "filename");
    if (qemu_shake_parsename(filename, si->name, MAX_SHAKE_IMG_NAME_SIZE - 1, &local_err) < 0) {
        error_propagate(errp, local_err);
        qemu_opts_del(opts);
        return -EINVAL;
    }

    // 启动shake
    shake_enable();

    //调用img_open,跟image连接
    si->img = NULL;
    si->img = img_open(si->name, 0, IO_CALL_BACK_MODE);
    if (si->img == NULL) {
        error_report("error open shake img");
        qemu_shake_cleanup(si);
        qemu_opts_del(opts);
        return -EIO;
    }
    /* error_report(" ===== open shake img successful!!!"); // TODO */
    return 0;
}

static void qemu_shake_close(BlockDriverState *bs)
{
    /* error_report(" ===== qemu_shake_close"); // TODO */
    ShakeImage *si = bs->opaque;
    if (si->img) img_close(si->img);
}

static const AIOCBInfo shake_aiocb_info = {
    .aiocb_size = sizeof(ShakeAIOCB),
};

// This function runs in qemu BH context.
static void shake_finish_bh(void *opaque)
{
    /* error_report(" ===== shake_finish_bh"); // TODO */
    ShakeAIOCB *acb = opaque;
    qemu_bh_delete(acb->bh);
    if (acb->cmd == SHAKE_AIO_READ) {
        //复制buf到qiov上
        //TODO 这个能否放到 img_iocb_t iocb的callback(shake thread) 里面去做
        qemu_iovec_from_buf(acb->qiov, 0, acb->buf, acb->qiov->size);
    }
    qemu_vfree(acb->buf);
    acb->common.cb(acb->common.opaque, (acb->ret == 0 ? 0 : -EIO)); // TODO we need think acb->ret
    qemu_aio_unref(acb);
}

/*
 * This is the callback function for shake_aio_read and _write
 *
 * Note: this function is being called from a non qemu thread(Shake thread)
 * so we need to be careful about what we do here. Generally we only
 * schedule a BH, and do the rest of the io completion handling
 * from shake_finish_bh() which runs in a qemu context.
 */
static void shake_finish_aiocb(img_iocb_t *iocb)
{
    /* error_report(" ===== shake_finish_aiocb"); // TODO */
    ShakeAIOCB *acb = img_get_iocb_udata(iocb);

    //获取result
    acb->ret = img_get_iocb_result(iocb);
    
    // schedule a BH, notify the QEMU IO thread to finish the bh
    acb->bh = aio_bh_new(bdrv_get_aio_context(acb->common.bs),
                         shake_finish_bh, acb);

    qemu_bh_schedule(acb->bh);
}

static BlockAIOCB *shake_start_aio(BlockDriverState *bs,
                                         int64_t sector_num,
                                         QEMUIOVector *qiov,
                                         int nb_sectors,
                                         BlockCompletionFunc *cb,
                                         void *opaque,
                                         ShakeAIOCmd cmd)
{
    /* error_report(" ===== shake_start_aio"); // TODO */
    ShakeImage *si = bs->opaque;
    ShakeAIOCB *acb = qemu_aio_get(&shake_aiocb_info, bs, cb, opaque);
    acb->cmd = cmd;
    acb->qiov = qiov;
    acb->ret = 0;
    acb->si = si;
    acb->bh = NULL;

    if (cmd == SHAKE_AIO_DISCARD || cmd == SHAKE_AIO_FLUSH) {
        acb->buf = NULL;
    } else {
        acb->buf = qemu_try_blockalign(bs, qiov->size);
        if (acb->buf == NULL) {
            goto failed;
        }
    }
    if (cmd == SHAKE_AIO_WRITE) {
        qemu_iovec_to_buf(acb->qiov, 0, acb->buf, qiov->size);
    }

    int off = sector_num * BDRV_SECTOR_SIZE;
    int size = nb_sectors * BDRV_SECTOR_SIZE;
    switch (cmd) {
        case SHAKE_AIO_WRITE:
            img_io_prep_pwrite(&acb->iocb, si->img, acb->buf, size, off, shake_finish_aiocb);
            break;
        case SHAKE_AIO_READ:
            img_io_prep_pread(&acb->iocb, si->img, acb->buf, size, off, shake_finish_aiocb);
            break;
        default:
            goto failed;
    }

    img_set_iocb_udata(&acb->iocb, acb);

    img_iocb_t *iocbs[1] = {&acb->iocb};
    img_io_submit(si->img, 1, iocbs);
    return &acb->common;

failed:
    qemu_vfree(acb->buf);
    qemu_aio_unref(acb);
    return NULL;
}

static BlockAIOCB *qemu_shake_aio_readv(BlockDriverState *bs,
                                              int64_t sector_num,
                                              QEMUIOVector *qiov,
                                              int nb_sectors,
                                              BlockCompletionFunc *cb,
                                              void *opaque)
{
    /* error_report(" ===== qemu_shake_aio_readv"); // TODO */
    return shake_start_aio(bs, sector_num, qiov, nb_sectors, cb, opaque,
                           SHAKE_AIO_READ);
}

static BlockAIOCB *qemu_shake_aio_writev(BlockDriverState *bs,
                                               int64_t sector_num,
                                               QEMUIOVector *qiov,
                                               int nb_sectors,
                                               BlockCompletionFunc *cb,
                                               void *opaque)
{
    /* error_report(" ===== qemu_shake_aio_writev"); // TODO */
    return shake_start_aio(bs, sector_num, qiov, nb_sectors, cb, opaque,
                           SHAKE_AIO_WRITE);
}

static int qemu_shake_getinfo(BlockDriverState *bs, BlockDriverInfo *bdi)
{
    /* error_report(" ===== qemu_shake_getinfo"); // TODO */
    ShakeImage *si = bs->opaque;
    bdi->cluster_size = si->img->obj_size;
    return 0;
}

static int64_t qemu_shake_getlength(BlockDriverState *bs)
{
    /* error_report(" ===== qemu_shake_getlength"); // TODO */
    ShakeImage *si = bs->opaque;
    si->img->size = 33391104; //TODO
    return si->img->size;
}

static int qemu_shake_truncate(BlockDriverState *bs, int64_t offset)
{
    /* error_report(" ===== qemu_shake_truncate"); // TODO */
    ShakeImage *si = bs->opaque;
    int r = img_resize(si->img, offset);
    if (r < 0) return r;
    return 0;
}

static int qemu_shake_create(const char *filename, QemuOpts *opts, Error **errp)
{
    /* error_report(" ===== qemu_shake_create"); // TODO */
    char name[MAX_SHAKE_IMG_NAME_SIZE];
    int64_t img_size = 0;
    int64_t obj_size = 0;
    int r = 0;
    Error *local_err = NULL;

    if (qemu_shake_parsename(filename,
                             name,
                             MAX_SHAKE_IMG_NAME_SIZE - 1,
                             &local_err) < 0) {
        error_propagate(errp, local_err);
        return -EINVAL;
    }

    /* Read out options */
    img_size = ROUND_UP(qemu_opt_get_size_del(opts, BLOCK_OPT_SIZE, 0),
                     BDRV_SECTOR_SIZE);
    obj_size = qemu_opt_get_size_del(opts, BLOCK_OPT_CLUSTER_SIZE, 0);
    if (obj_size) {
        if ((obj_size - 1) & obj_size) {    /* not a power of 2? */
            error_setg(errp, "obj size needs to be power of 2");
            return -EINVAL;
        }
        if (obj_size < 4096) {
            error_setg(errp, "obj size too small");
            return -EINVAL;
        }
    }

    //create img
    r = img_create(name, img_size, obj_size);
    if (r != 0) {
        error_report("error create shake img");
        r = -EIO;
    }

    return r;
}

static QemuOptsList qemu_shake_create_opts = {
    .name = "shake-create-opts",
    .head = QTAILQ_HEAD_INITIALIZER(qemu_shake_create_opts.head),
    .desc = {
        {
            .name = BLOCK_OPT_SIZE,
            .type = QEMU_OPT_SIZE,
            .help = "Virtual Disk Size"
        },
        {
            .name = BLOCK_OPT_CLUSTER_SIZE,
            .type = QEMU_OPT_SIZE,
            .help = "Shake Image Object Size"
        },
        { /* end of list */ }
    }
};

static BlockDriver bdrv_shake = {
    .format_name            = "shake",
    .protocol_name          = "shake",
    .instance_size          = sizeof(ShakeImage),
    .bdrv_needs_filename    = true,

    .bdrv_file_open         = qemu_shake_open,
    .bdrv_close             = qemu_shake_close,
    .bdrv_create            = qemu_shake_create,
    .bdrv_has_zero_init     = bdrv_has_zero_init_1,
    .bdrv_get_info          = qemu_shake_getinfo,
    .create_opts            = &qemu_shake_create_opts,
    .bdrv_getlength         = qemu_shake_getlength,
    .bdrv_truncate          = qemu_shake_truncate,

    .bdrv_aio_readv         = qemu_shake_aio_readv,
    .bdrv_aio_writev        = qemu_shake_aio_writev,

#if 0
    .bdrv_aio_flush         = qemu_shake_aio_flush,
    .bdrv_aio_discard       = qemu_shake_aio_discard,
    .bdrv_snapshot_create   = qemu_shake_snap_create,
    .bdrv_snapshot_delete   = qemu_shake_snap_remove,
    .bdrv_snapshot_list     = qemu_shake_snap_list,
    .bdrv_snapshot_goto     = qemu_shake_snap_rollback,
    .bdrv_invalidate_cache  = qemu_shake_invalidate_cache,
#endif
};

static void bdrv_shake_init(void)
{
    bdrv_register(&bdrv_shake);
}

block_init(bdrv_shake_init);
