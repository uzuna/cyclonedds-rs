// Cyclone DDS exports some inline functions that are needed by bindings.  Since these are not part of the library,
// we need to re-implement them as non-inline functions here.

#include "dds/ddsi/ddsi_serdata.h"
#include "dds/ddsc/dds_loaned_sample.h"

struct ddsi_serdata *ddsi_serdata_addref (const struct ddsi_serdata *serdata_const) {
#if defined (__cplusplus)
  DDSRT_WARNING_GNUC_OFF(old-style-cast)
  DDSRT_WARNING_CLANG_OFF(old-style-cast)
#endif
  struct ddsi_serdata *serdata = (struct ddsi_serdata *)serdata_const;
#if defined (__cplusplus)
  DDSRT_WARNING_CLANG_ON(old-style-cast)
  DDSRT_WARNING_GNUC_ON(old-style-cast)
#endif
  ddsrt_atomic_inc32 (&serdata->refc);
  return serdata;
}

void ddsi_serdata_removeref (struct ddsi_serdata *serdata) {
  if (ddsrt_atomic_dec32_ov (&serdata->refc) == 1)
    serdata->ops->free (serdata);
}

void dds_loaned_sample_addref (struct dds_loaned_sample *loaned_sample) {
  dds_loaned_sample_ref (loaned_sample);
}

void dds_loaned_sample_removeref (struct dds_loaned_sample *loaned_sample) {
  dds_loaned_sample_unref (loaned_sample);
}
