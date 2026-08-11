#include "dds/dds.h"
#include "dds/ddsi/ddsi_serdata.h"
#include "dds/ddsi/ddsi_sertype.h"
#include "dds/ddsi/ddsi_radmin.h"
#include "dds/ddsrt/md5.h"
#include "dds/ddsc/dds_loaned_sample.h"
#include "dds/ddsc/dds_psmx.h"

/*  dds_status_id_t is not used by any function so it doesn't turn up in the generated
    bindings. This dummy function forces the dds_status_id_t to be used.
 */

void _dummy(dds_status_id_t status);
// cyclone inline functions needed by bindings reimplemented here
struct ddsi_serdata *ddsi_serdata_addref(const struct ddsi_serdata *serdata_const);
void ddsi_serdata_removeref(struct ddsi_serdata *serdata);
void dds_loaned_sample_addref(struct dds_loaned_sample *loaned_sample);
void dds_loaned_sample_removeref(struct dds_loaned_sample *loaned_sample);

const int BUILTIN_TOPIC_DCPSPARTICIPANT = DDS_BUILTIN_TOPIC_DCPSPARTICIPANT;
const int BUILTIN_TOPIC_DCPSTOPIC = DDS_BUILTIN_TOPIC_DCPSTOPIC;
const int BUILTIN_TOPIC_DCPSPUBLICATION = DDS_BUILTIN_TOPIC_DCPSPUBLICATION;
const int BUILTIN_TOPIC_DCPSSUBSCRIPTION = DDS_BUILTIN_TOPIC_DCPSSUBSCRIPTION;
