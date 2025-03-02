package ryan.widodo

import java.security.Permission

class ExitException(status: Int)
    extends SecurityException(s"Exit status: $status") {
  def getStatus: Int = status
}

class NoExitSecurityManager extends SecurityManager {
  private val originalSecurityManager = System.getSecurityManager

  override def checkPermission(perm: Permission): Unit = {
    if (originalSecurityManager != null) {
      originalSecurityManager.checkPermission(perm)
    }
  }

  override def checkPermission(perm: Permission, context: Any): Unit = {
    if (originalSecurityManager != null) {
      originalSecurityManager.checkPermission(perm, context)
    }
  }

  override def checkExit(status: Int): Unit = {
    throw new ExitException(status)
  }
}
