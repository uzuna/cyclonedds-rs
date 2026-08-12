# deb向けArch
ARCH ?= $(shell uname -m)
# replace x86_64 to amd64
ifeq ($(ARCH),x86_64)
ARCH = amd64
endif
# replace aarch64 to arm64
ifeq ($(ARCH),aarch64)
ARCH = arm64
endif

# 共通の環境変数設定
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR := $(patsubst %/,%,$(dir $(mkfile_path)))

# deb出力dir
DEB_DIR := ${PROJECT_DIR}/deb

# vendorディレクトリ
VENDOR_DIR := ${PROJECT_DIR}/vendor
# shm動作に必要な調停機能を持つバイナリ
ICEORYX_BIN := ${VENDOR_DIR}/iceoryx/install/bin/iox-roudi
# cycloneddsの共有ライブラリ
CYCLONEDDS_LIB := ${VENDOR_DIR}/cyclonedds/install/lib/libddsc.so

# iceoryx2のCバインディング。psmx_iox2が静的リンクするため実行時の配布は不要
ICEORYX2_DIR := ${VENDOR_DIR}/iceoryx2
ICEORYX2_INSTALL := ${ICEORYX2_DIR}/target/ffi/install
ICEORYX2_LIB := ${ICEORYX2_INSTALL}/lib/libiceoryx2_ffi_c.a
# iceoryx2版のPSMXプラグイン。iox-roudiのような調停プロセスを必要としない
PSMX_IOX2_LIB := ${VENDOR_DIR}/cyclonedds/install/lib/libpsmx_iox2.so
