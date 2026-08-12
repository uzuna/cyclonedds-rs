include ../common.mk

.PHONY: build
build: ${CYCLONEDDS_LIB} ${ICEORYX_BIN}

# vendor以下のビルドに必要なパッケージ
.PHONY: setup
setup:
	sudo apt install -y cmake build-essential git gcc g++ pkg-config \
		libacl1-dev libncurses5-dev

${ICEORYX_BIN}:
	cd iceoryx \
	&& cmake -Bbuild -Hiceoryx_meta -DCMAKE_INSTALL_PREFIX=install \
	&& cmake --build build --config RelWithDebInfo --target install

${CYCLONEDDS_LIB}: ${ICEORYX_BIN}
	cd cyclonedds \
	&& mkdir -p build install \
	&& cmake -Bbuild -DENABLE_ICEORYX=YES \
		-DWERROR=OFF -DBUILD_IDLC=ON -DBUILD_DDSPERF=OFF -DBUILD_TESTING=OFF \
		-DBUILD_DDSPERF=OFF -DENABLE_TYPE_DISCOVERY=YES -DENABLE_TOPIC_DISCOVERY=YES \
		-DCMAKE_PREFIX_PATH=$(VENDOR_DIR)/iceoryx/install/ \
		-DCMAKE_INSTALL_PREFIX=install \
	&& cmake --build build --config RelWithDebInfo --target install

# Why opt-in: iceoryx2のビルドはcargoを挟むため、既定の`build`に入れるとCIのdebビルドまで
# 巻き込む。iceoryx2の評価が済むまでは明示的に叩いたときだけビルドする
.PHONY: build-iox2
build-iox2: ${PSMX_IOX2_LIB}

${ICEORYX2_LIB}:
	cd iceoryx2 \
	&& cargo build --release --package iceoryx2-ffi-c \
	&& cmake -S . -B target/ffi/build \
		-DCMAKE_INSTALL_PREFIX=$(ICEORYX2_INSTALL) \
		-DBUILD_CXX_BINDING=OFF \
		-DRUST_BUILD_ARTIFACT_PATH=$(ICEORYX2_DIR)/target/release \
	&& cmake --build target/ffi/build \
	&& cmake --install target/ffi/build

# iceoryx1と2の両プラグインを同居させ、XMLのPubSubMessageExchangeだけで経路を選べるようにする
${PSMX_IOX2_LIB}: ${ICEORYX_BIN} ${ICEORYX2_LIB}
	cd cyclonedds \
	&& mkdir -p build install \
	&& cmake -Bbuild -DENABLE_ICEORYX=YES -DENABLE_ICEORYX2=ON \
		-DWERROR=OFF -DBUILD_IDLC=ON -DBUILD_DDSPERF=OFF -DBUILD_TESTING=OFF \
		-DENABLE_TYPE_DISCOVERY=YES -DENABLE_TOPIC_DISCOVERY=YES \
		-DCMAKE_PREFIX_PATH="$(VENDOR_DIR)/iceoryx/install/;$(ICEORYX2_INSTALL)" \
		-DCMAKE_INSTALL_PREFIX=install \
	&& cmake --build build --config RelWithDebInfo --target install

.PHONY: clean
clean:
	rm -rf iceoryx/build iceoryx/install cyclonedds/build cyclonedds/install \
		iceoryx2/target
