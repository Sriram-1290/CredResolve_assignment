"""SmartDialer: a progressive/predictive outbound dialer prototype with a hard safety boundary.

Pipeline (see docs/ARCHITECTURE.md):

    Campaign -> Pacer (proposal only) -> SafetyController (mints DialPermit)
             -> CallAllocator (only code that places calls) -> TelecomProvider
"""
