package cityblock.utilities

// TODO docs
object Insurance {

  object LineOfBusiness extends Enumeration {
    val Commercial, Medicare, Medicaid, DSNP = Value
  }

  object SubLineOfBusiness extends Enumeration {
    val HMO, POS, PPO, DSNP, MedicareAdvantage, Medicaid, FullyInsured, Exchange, SelfFunded, Dual =
      Value
  }
}
