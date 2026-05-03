#
# Makefile for slurmbrokerd (cross-domain Slurm broker daemon, MVP).
#
# Modeled after src/sackd/Makefile.am: minimal sbin_PROGRAMS that links
# against libslurm. The systemd unit file is built and installed by
# etc/Makefile.am, alongside slurmctld.service / slurmd.service / etc.

AUTOMAKE_OPTIONS = foreign
CLEANFILES = core.*

AM_CPPFLAGS = -I$(top_srcdir)

sbin_PROGRAMS = slurmbrokerd

slurmbrokerd_SOURCES = \
	slurmbrokerd.c slurmbrokerd.h \
	broker_conf.c broker_conf.h \
	user_mapping.c user_mapping.h \
	broker_job.c broker_job.h \
	persist.c persist.h \
	proto.c proto.h \
	proto_pack.c \
	listener.c listener.h \
	handler_ctld.c handler_ctld.h \
	handler_remote.c handler_remote.h \
	egress.c egress.h \
	state_machine.c state_machine.h \
	sync_ticker.c sync_ticker.h \
	stage.c stage.h \
	rewrite.c rewrite.h

slurmbrokerd_DEPENDENCIES = $(LIB_SLURM_BUILD)
slurmbrokerd_LDFLAGS = $(CMD_LDFLAGS)
slurmbrokerd_LDADD = $(LIB_SLURM)

force:
$(slurmbrokerd_DEPENDENCIES) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
