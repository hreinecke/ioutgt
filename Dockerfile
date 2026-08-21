FROM registry.opensuse.org/opensuse/bci/rust
ENV IOUTGTSRC=/usr/src/ioutgt

WORKDIR $IOUTGTSRC
ADD . $IOUTGTSRC
RUN cargo build --release -p ioutgt-nvme-tcp
RUN cp $IOUTGTSRC/target/release/ioutgt-nvme-tcp /usr/sbin
